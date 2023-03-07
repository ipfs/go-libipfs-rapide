package rapide

import (
	"io"
	"sync"

	"github.com/ipfs/boxo/blocks"
	"github.com/ipfs/go-cid"
)

// optimize for 2MiB blocks at 10Gbit/s throughput and 200ms one way latency:
// 10Gbit/s * 200ms / 2MiB = 119.2; then round up to the nearest power of two
const targetParallelBlockDownloads = 128

type clientDrivenWorker struct {
	impl   ClientDrivenDownloader
	dl     *download
	outErr *error
	mu     sync.Mutex

	// len counts the number of non done nodes in the snake
	len  uint
	head *snake
	tail *snake

	// TODO: add a dontGoThere map which tells you what part of the dag this node is not able to handle
}

func (d *download) startClientDrivenWorker(impl ClientDrivenDownloader, start *node, outErr *error) {
	w := &clientDrivenWorker{
		impl:   impl,
		dl:     d,
		outErr: outErr,
		len:    1,
	}

	root := &snake{
		worker: w,
		node:   start,
	}

	w.head = root
	w.tail = root

	impl.Download(CidCallbackPair{start.cid, root.callback})
}

// err must be called while holding w.mu.
func (w *clientDrivenWorker) err(err error) {
	if err == io.EOF {
		w.dl.workerFinished()
	} else {
		*w.outErr = err
		w.dl.workerErrored()
	}

	toCancel := make([]cid.Cid, w.len)
	for i, p := 0, w.head; p != nil; i, p = i+1, p.children {
		if p.status != snakeTodo {
			continue
		}

		p.status = snakeDone
		toCancel[i] = p.node.cid
	}
	w.len = 0
	w.impl.Cancel(toCancel...)
}

type snakeStatus uint8

const (
	snakeTodo snakeStatus = iota
	snakeDone
	snakeDuped
)

type snake struct {
	worker   *clientDrivenWorker
	parent   *snake
	children *snake
	node     *node
	// level indicates how deep a node is in the tree
	level  uint
	status snakeStatus
}

func (s *snake) callback(data []byte, err error) {
	w := s.worker
	w.mu.Lock()
	defer w.mu.Unlock()
	if s.status > snakeTodo {
		// we already canceled this snake, do nothing
		return
	}
	w.len--
	s.status = snakeDone
	n := s.node

	if err != nil {
		// TODO: handle ErrNotFound
		goto Errr
	}

	n.mu.Lock()
	if n.state == todo {
		var block blocks.Block
		block, err = blocks.NewBlockWithCid(data, n.cid)
		if err != nil {
			goto Errr
		}
		err = n.expand(w.dl, block)
		n.mu.Lock()
		if err != nil {
			goto Errr
		}

		newBlocksWanted := uint(len(n.childrens))
		if remainingSpace := targetParallelBlockDownloads - w.len; newBlocksWanted > remainingSpace {
			newBlocksWanted = remainingSpace
		}
		w.len += newBlocksWanted
		if newBlocksWanted != 0 {
			downloads := make([]CidCallbackPair, newBlocksWanted)
			// TODO: select blocks randomly within the children
			left, right := s.dup()
			for i := range downloads {
				child := n.childrens[i]
				child.mu.Lock()
				child.workers++
				child.mu.Unlock()
				ns := &snake{
					worker: s.worker,
					node:   child,
					parent: left,
					level:  s.level + 1,
				}
				left.children, left = ns, ns
				downloads[i] = CidCallbackPair{child.cid, ns.callback}
			}
			left.children, right.parent = right, left
			s.update()
			// TODO: if we havn't found enough blocks to download, try in other nodes of the snake or try backtracking from the head.
			s.worker.impl.Download(downloads...)
		} else {
			s.update()
		}
		select {
		case w.dl.out <- blocks.Is(block):
		case <-w.dl.ctx.Done():
			err = w.dl.ctx.Err()
			goto Errr
		}
		return
	}
	// duplicated block
	s.update()
	return

Errr:
	s.update()
	w.err(err)
}

// update checks if this node should be removed from the snake and do so if needed. It will update the metric if needed.
// It must be called while holding s.worker.mu and s.node.mu, it will unlock s.node.mu.
func (s *snake) update() {
	if s.status == snakeTodo {
		s.node.mu.Unlock()
		return
	}
	removeSelf := (s.parent == nil || s.parent.level <= s.level) && (s.children == nil || s.children.level <= s.level)
	if !removeSelf {
		s.node.mu.Unlock()
		return
	}
	if s.parent != nil {
		s.parent.children = s.children
	} else {
		s.worker.head = s.children
	}
	if s.children != nil {
		s.children.parent = s.parent
	} else {
		s.worker.tail = s.parent
	}
	if s.status == snakeDone {
		s.node.workers--
	}
	s.node.mu.Unlock()
	if s.parent != nil {
		s.parent.updateWithoutNodeLock(updateParent)
		s.parent = nil
	}
	if s.children != nil {
		s.children.updateWithoutNodeLock(updateChild)
		s.children = nil
	}
}

type updateDirection uint8

const (
	_ updateDirection = iota
	updateParent
	updateChild
)

// updateWithoutNodeLock is like update but it doesn't require to hold s.node.mu.
// It must be called while holding s.worker.mu.
func (s *snake) updateWithoutNodeLock(direction updateDirection) {
	if s.status == snakeTodo {
		return
	}
	removeSelf := (s.parent == nil || s.parent.level <= s.level) && (s.children == nil || s.children.level <= s.level)
	if !removeSelf {
		return
	}
	if s.parent != nil {
		s.parent.children = s.children
	} else {
		s.worker.head = s.children
	}
	if s.children != nil {
		s.children.parent = s.parent
	} else {
		s.worker.tail = s.parent
	}
	if s.status == snakeDone {
		s.node.mu.Lock()
		s.node.workers--
		s.node.mu.Unlock()
	}
	switch direction {
	case updateParent:
		if s.parent != nil {
			s.parent.updateWithoutNodeLock(direction)
		}
	case updateChild:
		if s.children != nil {
			s.children.updateWithoutNodeLock(direction)
		}
	default:
		panic("unreachable")
	}
	s.parent, s.children = nil, nil
}

// dup duplicate this node of the snake and return left and right pointer.
// This allows to insert children nodes inbetween.
func (s *snake) dup() (*snake, *snake) {
	if s.status != snakeDone {
		panic("trying to dup an not done snake")
	}
	scopy := *s
	s2 := &scopy
	if s.children != nil {
		s.children.parent = s2
	}
	s2.parent, s.children = s, s2
	return s, s2
}
