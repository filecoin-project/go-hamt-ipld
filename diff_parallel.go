package hamt

import (
	"bytes"
	"context"
	"sync"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

// ParallelDiff returns a set of changes that transform node 'prev' into node 'cur'. opts are applied to both prev and cur.
func ParallelDiff(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, workers int64, opts ...Option) ([]*Change, error) {
	if prev.Equals(cur) {
		return nil, nil
	}

	prevHamt, err := LoadNode(ctx, prevBs, prev, opts...)
	if err != nil {
		return nil, err
	}

	curHamt, err := LoadNode(ctx, curBs, cur, opts...)
	if err != nil {
		return nil, err
	}

	if curHamt.bitWidth != prevHamt.bitWidth {
		return nil, xerrors.Errorf("diffing HAMTs with differing bitWidths not supported (prev=%d, cur=%d)", prevHamt.bitWidth, curHamt.bitWidth)
	}

	return doParallelDiffNode(ctx, prevHamt, curHamt, workers)
}

func doParallelDiffNode(ctx context.Context, pre, cur *Node, workers int64) ([]*Change, error) {
	bp := cur.Bitfield.BitLen()
	if pre.Bitfield.BitLen() > bp {
		bp = pre.Bitfield.BitLen()
	}

	initTasks := []*task{}
	for idx := bp; idx >= 0; idx-- {
		preBit := pre.Bitfield.Bit(idx)
		curBit := cur.Bitfield.Bit(idx)
		initTasks = append(initTasks, &task{
			idx:    idx,
			pre:    pre,
			preBit: preBit,
			cur:    cur,
			curBit: curBit,
		})
	}

	out := make(chan *Change, 2*workers)
	differ, ctx := newDiffScheduler(ctx, workers, initTasks...)
	differ.startWorkers(ctx, out)
	differ.startScheduler(ctx)

	var changes []*Change
	done := make(chan struct{})
	go func() {
		defer close(done)
		for change := range out {
			changes = append(changes, change)
		}
	}()

	err := differ.grp.Wait()
	close(out)
	<-done

	return changes, err
}

type task struct {
	idx int

	pre    *Node
	preBit uint

	cur    *Node
	curBit uint
}

func newDiffScheduler(ctx context.Context, numWorkers int64, rootTasks ...*task) (*diffScheduler, context.Context) {
	grp, ctx := errgroup.WithContext(ctx)
	s := &diffScheduler{
		numWorkers: numWorkers,
		stack:      rootTasks,
		in:         make(chan *task, numWorkers),
		out:        make(chan *task, numWorkers),
		grp:        grp,
	}
	s.taskWg.Add(len(rootTasks))
	return s, ctx
}

type diffScheduler struct {
	// number of worker routine to spawn
	numWorkers int64
	// buffer holds tasks until they are processed
	stack []*task
	// inbound and outbound tasks
	in, out chan *task
	// tracks number of inflight tasks
	taskWg sync.WaitGroup
	// launches workers and collects errors if any occur
	grp *errgroup.Group
}

func (s *diffScheduler) enqueueTask(task *task) {
	s.taskWg.Add(1)
	s.in <- task
}

func (s *diffScheduler) startScheduler(ctx context.Context) {
	s.grp.Go(func() error {
		defer func() {
			close(s.out)
			// Because the workers may have exited early (due to the context being canceled).
			for range s.out {
				s.taskWg.Done()
			}
			// Because the workers may have enqueued additional tasks.
			for range s.in {
				s.taskWg.Done()
			}
			// now, the waitgroup should be at 0, and the goroutine that was _waiting_ on it should have exited.
		}()
		go func() {
			s.taskWg.Wait()
			close(s.in)
		}()
		for {
			if n := len(s.stack) - 1; n >= 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				case s.out <- s.stack[n]:
					s.stack[n] = nil
					s.stack = s.stack[:n]
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				}
			}
		}
	})
}

func (s *diffScheduler) startWorkers(ctx context.Context, out chan *Change) {
	for i := int64(0); i < s.numWorkers; i++ {
		s.grp.Go(func() error {
			for task := range s.out {
				if err := s.work(ctx, task, out); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func (s *diffScheduler) work(ctx context.Context, todo *task, results chan *Change) error {
	defer s.taskWg.Done()
	idx := todo.idx
	preBit := todo.preBit
	pre := todo.pre
	curBit := todo.curBit
	cur := todo.cur

	switch {
	case preBit == 1 && curBit == 1:
		// index for pre and cur will be unique to each, calculate it here.
		prePointer := pre.getPointer(byte(pre.indexForBitPos(idx)))
		curPointer := cur.getPointer(byte(cur.indexForBitPos(idx)))
		switch {
		// both pointers are shards, recurse down the tree.
		case prePointer.isShard() && curPointer.isShard():
			preChild, err := prePointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			curChild, err := curPointer.loadChild(ctx, cur.store, cur.bitWidth, cur.hash)
			if err != nil {
				return err
			}

			bp := curChild.Bitfield.BitLen()
			if preChild.Bitfield.BitLen() > bp {
				bp = preChild.Bitfield.BitLen()
			}
			for idx := bp; idx >= 0; idx-- {
				preBit := preChild.Bitfield.Bit(idx)
				curBit := curChild.Bitfield.Bit(idx)
				s.enqueueTask(&task{
					idx:    idx,
					pre:    preChild,
					preBit: preBit,
					cur:    curChild,
					curBit: curBit,
				})
			}

		// check if KV's from cur exists in any children of pre's child.
		case prePointer.isShard() && !curPointer.isShard():
			childKV, err := prePointer.loadChildKVs(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			parallelDiffKVs(childKV, curPointer.KVs, results)

		// check if KV's from pre exists in any children of cur's child.
		case !prePointer.isShard() && curPointer.isShard():
			childKV, err := curPointer.loadChildKVs(ctx, cur.store, cur.bitWidth, cur.hash)
			if err != nil {
				return err
			}
			parallelDiffKVs(prePointer.KVs, childKV, results)

		// both contain KVs, compare.
		case !prePointer.isShard() && !curPointer.isShard():
			parallelDiffKVs(prePointer.KVs, curPointer.KVs, results)
		}
	case preBit == 1 && curBit == 0:
		// there exists a value in previous not found in current - it was removed
		pointer := pre.getPointer(byte(pre.indexForBitPos(idx)))

		if pointer.isShard() {
			child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			err = parallelRemoveAll(ctx, child, results)
			if err != nil {
				return err
			}
		} else {
			for _, p := range pointer.KVs {
				results <- &Change{
					Type:   Remove,
					Key:    string(p.Key),
					Before: p.Value,
					After:  nil,
				}
			}
		}
	case preBit == 0 && curBit == 1:
		// there exists a value in current not found in previous - it was added
		pointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

		if pointer.isShard() {
			child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
			if err != nil {
				return err
			}
			err = parallelAddAll(ctx, child, results)
			if err != nil {
				return err
			}
		} else {
			for _, p := range pointer.KVs {
				results <- &Change{
					Type:   Add,
					Key:    string(p.Key),
					Before: nil,
					After:  p.Value,
				}
			}
		}
	}
	return nil
}

func parallelDiffKVs(pre, cur []*KV, out chan *Change) {
	preMap := make(map[string]*cbg.Deferred, len(pre))
	curMap := make(map[string]*cbg.Deferred, len(cur))

	for _, kv := range pre {
		preMap[string(kv.Key)] = kv.Value
	}
	for _, kv := range cur {
		curMap[string(kv.Key)] = kv.Value
	}
	// find removed keys: keys in pre and not in cur
	for key, value := range preMap {
		if _, ok := curMap[key]; !ok {
			out <- &Change{
				Type:   Remove,
				Key:    key,
				Before: value,
				After:  nil,
			}
		}
	}
	// find added keys: keys in cur and not in pre
	// find modified values: keys in cur and pre with different values
	for key, curVal := range curMap {
		if preVal, ok := preMap[key]; !ok {
			out <- &Change{
				Type:   Add,
				Key:    key,
				Before: nil,
				After:  curVal,
			}
		} else {
			if !bytes.Equal(preVal.Raw, curVal.Raw) {
				out <- &Change{
					Type:   Modify,
					Key:    key,
					Before: preVal,
					After:  curVal,
				}
			}
		}
	}
}

func parallelAddAll(ctx context.Context, node *Node, out chan *Change) error {
	return node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		out <- &Change{
			Type:   Add,
			Key:    k,
			Before: nil,
			After:  val,
		}
		return nil
	})
}

func parallelRemoveAll(ctx context.Context, node *Node, out chan *Change) error {
	return node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		out <- &Change{
			Type:   Remove,
			Key:    k,
			Before: val,
			After:  nil,
		}
		return nil
	})
}
