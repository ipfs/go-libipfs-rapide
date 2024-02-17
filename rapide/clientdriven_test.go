package rapide_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/ipfs/boxo/rapide"
	"github.com/ipfs/go-cid"
)

type mockClientDrivenDownloder struct {
	bs *mockBlockstore
	mu sync.Mutex
	m  map[cid.Cid]context.CancelFunc
}

func (m *mockClientDrivenDownloder) Download(ccs ...CidCallbackPair) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, d := range ccs {
		ctx, cancel := context.WithCancel(context.Background())
		cid := d.Cid
		cb := d.Callback
		m.m[cid] = cancel
		go func() {
			b, err := m.bs.GetBlock(ctx, cid)
			cb(b.RawData(), err)
			m.mu.Lock()
			delete(m.m, cid)
			m.mu.Unlock()
		}()
	}
}

func (m *mockClientDrivenDownloder) Cancel(cids ...cid.Cid) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, c := range cids {
		cancel, ok := m.m[c]
		if !ok {
			continue
		}
		cancel()
	}
}

func TestClientDrivenDownloader(t *testing.T) {
	t.Parallel()
	for _, tc := range [...]struct {
		delay   time.Duration
		runners uint
		width   uint
		depth   uint
	}{
		{0, 1, 2, 2},
		{time.Nanosecond, 1, 2, 2},
		{time.Microsecond, 1, 2, 2},
		{time.Millisecond, 1, 2, 2},
	} {
		tc := tc
		t.Run(fmt.Sprintf("%v %v %v %v", tc.delay, tc.runners, tc.width, tc.depth), func(t *testing.T) {
			t.Parallel()
			bs := &mockBlockstore{
				t:     t,
				delay: tc.delay,
			}
			var i uint64
			root := bs.makeDag(tc.width, tc.depth, &i)

			clients := make([]ClientDrivenDownloader, tc.runners)
			for i := tc.runners; i != 0; {
				i--
				clients[i] = &mockClientDrivenDownloder{bs: bs, m: make(map[cid.Cid]context.CancelFunc)}
			}

			seen := make(map[cid.Cid]struct{})
			for b := range (&Client{ClientDrivenDownloaders: clients}).Get(context.Background(), root, bs) {
				block, err := b.Get()
				if err != nil {
					t.Fatalf("got error from rapide: %s", err)
				}
				c := block.Cid()
				if _, ok := bs.m[c]; !ok {
					t.Fatalf("got cid not in blockstore %s", c)
				}
				seen[c] = struct{}{}
			}

			if len(seen) != len(bs.m) {
				t.Fatalf("seen less blocks than in blockstore: expected %d; got %d", len(bs.m), len(seen))
			}
		})
	}
}
