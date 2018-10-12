package hamt

import (
	"context"
	"fmt"
	"math/big"

	cid "github.com/ipfs/go-cid"
	murmur3 "github.com/spaolacci/murmur3"
)

const arrayWidth = 3

type Node struct {
	Bitfield *big.Int   `refmt:"bf"`
	Pointers []*Pointer `refmt:"p"`

	// for fetching and storing children
	store *CborIpldStore
}

func NewNode(cs *CborIpldStore) *Node {
	return &Node{
		Bitfield: big.NewInt(0),
		Pointers: make([]*Pointer, 0),
		store:    cs,
	}
}

type KV struct {
	Key   string
	Value interface{}
}

type Pointer struct {
	KVs  []*KV   `refmt:"v,omitempty"`
	Link cid.Cid `refmt:"l,omitempty"`

	// cached node to avoid too many serialization operations
	cache *Node
}

var hash = func(k string) []byte {
	h := murmur3.New128()
	h.Write([]byte(k))
	return h.Sum(nil)
}

func (n *Node) Find(ctx context.Context, k string) (interface{}, error) {
	var out interface{}
	err := n.getValue(ctx, hash(k), 0, k, func(kv *KV) error {
		out = kv.Value
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (n *Node) Delete(ctx context.Context, k string) error {
	return n.modifyValue(ctx, hash(k), 0, k, nil)
}

var ErrNotFound = fmt.Errorf("not found")

func (n *Node) getValue(ctx context.Context, hv []byte, depth int, k string, cb func(*KV) error) error {
	idx := hv[depth]
	if n.Bitfield.Bit(int(idx)) == 0 {
		return ErrNotFound
	}

	cindex := byte(n.indexForBitPos(int(idx)))

	c := n.getChild(cindex)
	if c.isShard() {
		chnd, err := c.loadChild(ctx, n.store)
		if err != nil {
			return err
		}

		return chnd.getValue(ctx, hv, depth+1, k, cb)
	}

	for _, kv := range c.KVs {
		if kv.Key == k {
			return cb(kv)
		}
	}

	return ErrNotFound
}

func (p *Pointer) loadChild(ctx context.Context, ns *CborIpldStore) (*Node, error) {
	if p.cache != nil {
		return p.cache, nil
	}

	out, err := LoadNode(ctx, ns, p.Link)
	if err != nil {
		return nil, err
	}

	p.cache = out
	return out, nil
}

func LoadNode(ctx context.Context, cs *CborIpldStore, c cid.Cid) (*Node, error) {
	var out Node
	if err := cs.Get(ctx, c, &out); err != nil {
		return nil, err
	}

	out.store = cs
	return &out, nil
}

func (n *Node) checkSize(ctx context.Context) (uint64, error) {
	c, err := n.store.Put(ctx, n)
	if err != nil {
		return 0, err
	}

	blk, err := n.store.Blocks.GetBlock(ctx, c)
	if err != nil {
		return 0, err
	}

	totsize := uint64(len(blk.RawData()))
	for _, ch := range n.Pointers {
		if ch.isShard() {
			chnd, err := ch.loadChild(ctx, n.store)
			if err != nil {
				return 0, err
			}
			chsize, err := chnd.checkSize(ctx)
			if err != nil {
				return 0, err
			}
			totsize += chsize
		}
	}

	return totsize, nil
}

func (n *Node) Flush(ctx context.Context) error {
	for _, p := range n.Pointers {
		if p.cache != nil {
			if err := p.cache.Flush(ctx); err != nil {
				return err
			}

			c, err := n.store.Put(ctx, p.cache)
			if err != nil {
				return err
			}

			p.cache = nil
			p.Link = c
		}
	}
	return nil
}

func (n *Node) Set(ctx context.Context, k string, v interface{}) error {
	return n.modifyValue(ctx, hash(k), 0, k, v)
}

func (n *Node) cleanChild(chnd *Node, cindex byte) error {
	l := len(chnd.Pointers)
	switch {
	case l == 0:
		return fmt.Errorf("incorrectly formed HAMT")
	case l == 1:
		// TODO: only do this if its a value, cant do this for shards unless pairs requirements are met.

		ps := chnd.Pointers[0]
		if ps.isShard() {
			return nil
		}

		return n.setChild(cindex, ps)
	case l <= arrayWidth:
		var chvals []*KV
		for _, p := range chnd.Pointers {
			if p.isShard() {
				return nil
			}

			for _, sp := range p.KVs {
				if len(chvals) == arrayWidth {
					return nil
				}
				chvals = append(chvals, sp)
			}
		}
		return n.setChild(cindex, &Pointer{KVs: chvals})
	default:
		return nil
	}
}

func (n *Node) modifyValue(ctx context.Context, hv []byte, depth int, k string, v interface{}) error {
	idx := int(hv[depth])

	if n.Bitfield.Bit(idx) != 1 {
		return n.insertChild(idx, k, v)
	}

	cindex := byte(n.indexForBitPos(idx))

	child := n.getChild(cindex)
	if child.isShard() {
		chnd, err := child.loadChild(ctx, n.store)
		if err != nil {
			return err
		}

		if err := chnd.modifyValue(ctx, hv, depth+1, k, v); err != nil {
			return err
		}

		// CHAMP optimization, ensure trees look correct after deletions
		if v == nil {
			if err := n.cleanChild(chnd, cindex); err != nil {
				return err
			}
		}

		return nil
	}

	if v == nil {
		for i, p := range child.KVs {
			if p.Key == k {
				if len(child.KVs) == 1 {
					return n.rmChild(cindex, idx)
				}

				copy(child.KVs[i:], child.KVs[i+1:])
				child.KVs = child.KVs[:len(child.KVs)-1]
				return nil
			}
		}
		return ErrNotFound
	}

	// check if key already exists
	for _, p := range child.KVs {
		if p.Key == k {
			p.Value = v
			return nil
		}
	}

	// If the array is full, create a subshard and insert everything into it
	if len(child.KVs) >= arrayWidth {
		sub := NewNode(n.store)
		if err := sub.modifyValue(ctx, hv, depth+1, k, v); err != nil {
			return err
		}

		for _, p := range child.KVs {
			if err := sub.modifyValue(ctx, hash(p.Key), depth+1, p.Key, p.Value); err != nil {
				return err
			}
		}

		c, err := n.store.Put(ctx, sub)
		if err != nil {
			return err
		}

		return n.setChild(cindex, &Pointer{Link: c})
	}

	// otherwise insert the new element into the array in order
	np := &KV{Key: k, Value: v}
	for i := 0; i < len(child.KVs); i++ {
		if k < child.KVs[i].Key {
			child.KVs = append(child.KVs[:i], append([]*KV{np}, child.KVs[i:]...)...)
			return nil
		}
	}
	child.KVs = append(child.KVs, np)
	return nil
}

func (n *Node) insertChild(idx int, k string, v interface{}) error {
	if v == nil {
		return ErrNotFound
	}

	i := n.indexForBitPos(idx)
	n.Bitfield.SetBit(n.Bitfield, idx, 1)

	p := &Pointer{KVs: []*KV{{Key: k, Value: v}}}

	n.Pointers = append(n.Pointers[:i], append([]*Pointer{p}, n.Pointers[i:]...)...)
	return nil
}

func (n *Node) setChild(i byte, p *Pointer) error {
	n.Pointers[i] = p
	return nil
}

func (n *Node) rmChild(i byte, idx int) error {
	copy(n.Pointers[i:], n.Pointers[i+1:])
	n.Pointers = n.Pointers[:len(n.Pointers)-1]
	n.Bitfield.SetBit(n.Bitfield, idx, 0)

	return nil
}

func (n *Node) getChild(i byte) *Pointer {
	if int(i) >= len(n.Pointers) || i < 0 {
		return nil
	}

	return n.Pointers[i]
}

func (n *Node) Copy() *Node {
	nn := NewNode(n.store)
	nn.Bitfield.Set(n.Bitfield)
	nn.Pointers = make([]*Pointer, len(n.Pointers))

	for i, p := range n.Pointers {
		pp := &Pointer{}
		if p.cache != nil {
			pp.cache = p.cache.Copy()
		}
		pp.Link = p.Link
		if p.KVs != nil {
			pp.KVs = make([]*KV, len(p.KVs))
			for j, kv := range p.KVs {
				pp.KVs[j] = &KV{Key: kv.Key, Value: kv.Value}
			}
		}
		nn.Pointers[i] = pp
	}

	return nn
}

func (p *Pointer) isShard() bool {
	return p.Link.Defined()
}
