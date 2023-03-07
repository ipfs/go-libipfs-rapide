package rapide

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/ipfs/boxo/blocks"
	"github.com/ipfs/boxo/ipsl"
	"github.com/ipfs/go-cid"
	"go.uber.org/multierr"
)

// TODO: Add ordering garentees in the API when we figureout signaling for ordering in the protocol.
type ServerDrivenDownloader interface {
	// When an error is seen on the channel, it is assumed that no more blocks will ever be received.
	// Clients are not required to perform traversal validation, the RAPIDE client will takes care of this.
	// Clients are required to validate hashes.
	Download(context.Context, cid.Cid, ipsl.Traversal) (ClosableBlockIterator, error)
}

// ClosableBlockIterator is an interator that implements io.Closer, we will always cancel the context
// and call close when stopping a request.
type ClosableBlockIterator interface {
	io.Closer
	blocks.BlockIterator
}

type ClientDrivenDownloader interface {
	// Download must be asynchronous. It schedule blocks to be downloaded and
	// callbacks to be called when either it failed or succeeded.
	// Clients need to callback when they have blocks or error.
	// In the callback either []byte != nil and error == nil or error != nil and []byte == nil.
	// When a callback for a cid is called the CID is cancel regardless of the success.
	// The callback is expected to be really fast and should be called synchronously.
	// All callbacks are threadsafe and may be called concurrently. But consumers
	// must not go out of their way to call them concurrently. Just do whatever
	// your underlying impl already do.
	Download(...CidCallbackPair)

	// Cancel must be asynchronous.
	// It indicates that we are no longer intrested in some blocks.
	// The callbacks are still allowed to be called again but that really not advised.
	// It would be nice if consumers freed the callbacks in the short term after this is called.
	Cancel(...cid.Cid)
}

type CidCallbackPair struct {
	Cid      cid.Cid
	Callback ClientDrivenCallback
}

type ClientDrivenCallback = func([]byte, error)

// A Client is a collection of routers and protocols that can be used to do requests.
type Client struct {
	ServerDrivenDownloaders []ServerDrivenDownloader
	ClientDrivenDownloaders []ClientDrivenDownloader
}

func (c *Client) Get(ctx context.Context, root cid.Cid, traversal ipsl.Traversal) <-chan blocks.BlockOrError {
	totalWorkers := uint(len(c.ServerDrivenDownloaders) + len(c.ClientDrivenDownloaders))
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan blocks.BlockOrError)
	d := &download{
		out:    out,
		ctx:    ctx,
		cancel: cancel,
		done:   uint64(totalWorkers),
		root: node{
			state:     todo,
			workers:   totalWorkers,
			cid:       root,
			traversal: traversal,
		},
		errors: make([]error, totalWorkers),
	}

	for i, cdd := range c.ClientDrivenDownloaders {
		d.startClientDrivenWorker(cdd, &d.root, &d.errors[i])
	}

	for i, sdd := range c.ServerDrivenDownloaders {
		d.startServerDrivenWorker(ctx, sdd, &d.root, &d.errors[len(c.ClientDrivenDownloaders)+i])
	}

	return out
}

type download struct {
	done      uint64 // done must be first due to 64bits types allignement issues on 32bits
	out       chan<- blocks.BlockOrError
	ctx       context.Context
	cancel    context.CancelFunc
	root      node
	errors    []error
	closeOnce sync.Once
}

// err cuts out the download and make it return an error, this is intended for unrecoverable errors.
func (d *download) err(err error) {
	d.closeOnce.Do(func() {
		select {
		case d.out <- blocks.IsNot(err):
		case <-d.ctx.Done():
		}
		d.cancel()
		close(d.out)
	})
}

func (d *download) finish() {
	d.closeOnce.Do(func() {
		d.cancel()
		close(d.out)
	})
}

func (d *download) workerFinished() {
	var minusOne uint64
	minusOne--
	if atomic.AddUint64(&d.done, minusOne) == 0 {
		d.finish()
	}
}

func (d *download) workerErrored() {
	var minusOne uint64
	minusOne--
	if atomic.AddUint64(&d.done, minusOne) == 0 {
		// we were the last worker, error
		d.err(multierr.Combine(d.errors...))
	}
}

type node struct {
	// parent is not protected by the mutex and is readonly after creation
	parent *node
	// cid is not protected by the mutex and is readonly after creation
	cid cid.Cid
	// to avoid ABBA lock ordering issues it is prohibited to grab the lock of the parent while holding the lock of the child.
	// it is also prohibited to grab the mutex of two different parts of the tree without holding all intermediary nodes for the same reason.
	mu sync.Mutex
	// traversal will be nilled out when the nodes has been explored.
	traversal ipsl.Traversal
	childrens []*node
	workers   uint
	state     nodeState
}

// expand will run the Traversal and create childrens, it must be called while holding n.mu.
// it will unlock n.mu.
func (n *node) expand(d *download, b blocks.Block) error {
	if n.state != todo {
		panic(fmt.Sprintf("expanding a node that is not todo: %d", n.state))
	}

	newResults, err := n.traversal.Traverse(b)
	if err != nil {
		d.err(err)
		n.mu.Unlock()
		return err
	}

	n.state = done
	n.traversal = nil // early gc

	childrens := make([]*node, len(newResults))
	for i, r := range newResults {
		childrens[i] = &node{
			state:     todo,
			parent:    n,
			cid:       r.Cid,
			traversal: r.Traversal,
		}
	}
	n.childrens = childrens

	// bubble up node removal
	node, parent := n, n.parent
	for {
		haveChildrens := len(node.childrens) != 0
		node.mu.Unlock()

		if haveChildrens {
			break
		}

		if parent == nil {
			// finished!
			return io.EOF
		}

		// nothing to do, backtrack
		parent.mu.Lock()
		for i, v := range parent.childrens {
			if v != node {
				continue
			}

			childrens := append(parent.childrens[:i], parent.childrens[i+1:]...)
			parent.childrens = append(childrens, nil)[:len(childrens)] // null out for gc
			break
		}

		node, parent = parent, parent.parent
	}

	return nil
}

type nodeState uint8

const (
	_ nodeState = iota
	// done indicates that the current node has been downloaded but it doesn't indicates that this part of the tree is complete
	// we remove completed parts of the tree from the lists.
	done
	// todo indicates that the node should be downloaded, a node that is in progress will still showup in todo
	// but it will have a non zero amount of workers
	todo
)
