package hamt

import (
	"context"
	"testing"
)

func TestRoundtrip(t *testing.T) {
	ctx := context.Background()

	n := NewNode()
	n.Bitfield.SetBit(n.Bitfield, 5, 1)
	n.Bitfield.SetBit(n.Bitfield, 7, 1)
	n.Bitfield.SetBit(n.Bitfield, 18, 1)

	n.Pointers = []*Pointer{{KVs: []*KV{{Key: "foo", Value: "bar"}}}}

	cs := NewCborStore()
	c, err := cs.Put(ctx, n)
	if err != nil {
		t.Fatal(err)
	}

	var nnode Node
	if err := cs.Get(ctx, c, &nnode); err != nil {
		t.Fatal(err)
	}

	c2, err := cs.Put(ctx, &nnode)
	if err != nil {
		t.Fatal(err)
	}

	if !c.Equals(c2) {
		t.Fatal("cid mismatch")
	}
}
