package hamt

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

var log = logging.Logger("diff")

// ChangeType denotes type of change in Change
type ChangeType int

// These constants define the changes that can be applied to a DAG.
const (
	Add ChangeType = iota
	Remove
	Modify
)

// Change represents a change to a DAG and contains a reference to the old and
// new CIDs.
type Change struct {
	Type   ChangeType
	Key    string
	Before *cbg.Deferred
	After  *cbg.Deferred
}

func (ch Change) String() string {
	b, _ := json.Marshal(ch)
	return string(b)
}

// Diff returns a set of changes that transform node 'prev' into node 'cur'. opts are applied to both prev and cur.
func Diff(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, opts ...Option) ([]*Change, error) {
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
	return diffNode(ctx, prevHamt, curHamt, 0)
}

func diffNode(ctx context.Context, pre, cur *Node, depth int) ([]*Change, error) {
	if pre == cur {
		return nil, nil
	}

	intersect := big.NewInt(0).And(cur.Bitfield, pre.Bitfield)

	// which Bitfield contains the most bits. We will start a loop from this index, calling Bitfield.Bit(idx)
	// on an out of range index will return zero.
	bp := intersect.BitLen()
	if pre.Bitfield.BitLen() > cur.Bitfield.BitLen() {
		bp = pre.Bitfield.BitLen()
	}
	if pre.Bitfield.BitLen() < cur.Bitfield.BitLen() {
		bp = cur.Bitfield.BitLen()
	}

	// the changes between cur and prev
	var changes []*Change

	// loop over each bit in the bitfields
	for idx := bp; idx >= 0; idx-- {
		// intersection across all fields means there _may_ exist a matching key in the pointers or
		// a child of a pointer.
		if intersect.Bit(idx) == 1 && pre.Bitfield.Bit(idx) == 1 && cur.Bitfield.Bit(idx) == 1 {
			// index for pre and cur will be unique to each, calculate it here.
			prePointer := pre.getPointer(byte(pre.indexForBitPos(idx)))
			curPointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

			// both pointers are shards, recurse down the tree.
			if prePointer.isShard() && curPointer.isShard() {
				preChild, err := prePointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				curChild, err := curPointer.loadChild(ctx, cur.store, cur.bitWidth, cur.hash)
				if err != nil {
					return nil, err
				}

				change, err := diffNode(ctx, preChild, curChild, depth+1)
				if err != nil {
					return nil, err
				}
				changes = append(changes, change...)
			}

			// check if KV's from cur exists in any children of pre's child.
			if prePointer.isShard() && !curPointer.isShard() {
				childKV, err := prePointer.loadChildKVs(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				changes = append(changes, diffKVs(childKV, curPointer.KVs, idx)...)

			}

			// check if KV's from pre exists in any children of cur's child.
			if !prePointer.isShard() && curPointer.isShard() {
				childKV, err := curPointer.loadChildKVs(ctx, cur.store, cur.bitWidth, cur.hash)
				if err != nil {
					return nil, err
				}
				changes = append(changes, diffKVs(prePointer.KVs, childKV, idx)...)
			}

			// both contain KVs, compare.
			if !prePointer.isShard() && !curPointer.isShard() {
				changes = append(changes, diffKVs(prePointer.KVs, curPointer.KVs, idx)...)
			}

		} else if pre.Bitfield.Bit(idx) == 1 && cur.Bitfield.Bit(idx) == 0 && intersect.Bit(idx) == 0 {
			// there exists a value in previous not found in current - it was removed
			pointer := pre.getPointer(byte(pre.indexForBitPos(idx)))

			if pointer.isShard() {
				child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				rm, err := removeAll(ctx, child, idx)
				if err != nil {
					return nil, err
				}
				changes = append(changes, rm...)
			} else {
				for _, p := range pointer.KVs {
					changes = append(changes, &Change{
						Type:   Remove,
						Key:    string(p.Key),
						Before: p.Value,
						After:  nil,
					})
					log.Infow("Change", "idx", idx, "type", "remove", "key", string(p.Key))
				}
			}

		} else if cur.Bitfield.Bit(idx) == 1 && pre.Bitfield.Bit(idx) == 0 && intersect.Bit(idx) == 0 {
			// there exists a value in current not found in previous - it was added
			pointer := cur.getPointer(byte(cur.indexForBitPos(idx)))

			if pointer.isShard() {
				child, err := pointer.loadChild(ctx, pre.store, pre.bitWidth, pre.hash)
				if err != nil {
					return nil, err
				}
				add, err := addAll(ctx, child, idx)
				if err != nil {
					return nil, err
				}
				changes = append(changes, add...)
			} else {
				for _, p := range pointer.KVs {
					changes = append(changes, &Change{
						Type:   Add,
						Key:    string(p.Key),
						Before: nil,
						After:  p.Value,
					})
					log.Infow("Change", "idx", idx, "type", "add", "key", string(p.Key))
				}
			}
		}
	}

	return changes, nil
}

func diffKVs(pre, cur []*KV, idx int) []*Change {
	preMap := make(map[string]*cbg.Deferred, len(pre))
	curMap := make(map[string]*cbg.Deferred, len(cur))
	var changes []*Change

	for _, kv := range pre {
		preMap[string(kv.Key)] = kv.Value
	}
	for _, kv := range cur {
		curMap[string(kv.Key)] = kv.Value
	}
	// find removed keys: keys in pre and not in cur
	for key, value := range preMap {
		if _, ok := curMap[key]; !ok {
			changes = append(changes, &Change{
				Type:   Remove,
				Key:    key,
				Before: value,
				After:  nil,
			})
			log.Infow("Change", "idx", idx, "type", "remove", "key", string(key))
		}
	}
	// find added keys: keys in cur and not in pre
	// find modified values: keys in cur and pre with different values
	for key, curVal := range curMap {
		if preVal, ok := preMap[key]; !ok {
			changes = append(changes, &Change{
				Type:   Add,
				Key:    key,
				Before: nil,
				After:  curVal,
			})
			log.Infow("Change", "idx", idx, "type", "add", "key", string(key))
		} else {
			if !bytes.Equal(preVal.Raw, curVal.Raw) {
				changes = append(changes, &Change{
					Type:   Modify,
					Key:    key,
					Before: preVal,
					After:  curVal,
				})
				log.Infow("Change", "idx", idx, "type", "modify", "key", string(key))
			}
		}
	}
	return changes
}

func addAll(ctx context.Context, node *Node, idx int) ([]*Change, error) {
	var changes []*Change
	if err := node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		changes = append(changes, &Change{
			Type:   Add,
			Key:    k,
			Before: nil,
			After:  val,
		})
		log.Infow("Change", "idx", idx, "type", "add", "key", string(k))

		return nil
	}); err != nil {
		return nil, err
	}
	return changes, nil
}

func removeAll(ctx context.Context, node *Node, idx int) ([]*Change, error) {
	var changes []*Change
	if err := node.ForEach(ctx, func(k string, val *cbg.Deferred) error {
		changes = append(changes, &Change{
			Type:   Remove,
			Key:    k,
			Before: val,
			After:  nil,
		})
		log.Infow("Change", "idx", idx, "type", "remove", "key", string(k))

		return nil
	}); err != nil {
		return nil, err
	}
	return changes, nil
}
