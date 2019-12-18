package hamt

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	cbor "github.com/ipfs/go-ipld-cbor"
)

func TestRoundtrip(t *testing.T) {
	ctx := context.Background()

	cs := NewCborStore()
	n := NewNode(cs)
	n.Bitfield.SetBit(n.Bitfield, 5, 1)
	n.Bitfield.SetBit(n.Bitfield, 7, 1)
	n.Bitfield.SetBit(n.Bitfield, 18, 1)

	d := &cbg.Deferred{Raw: []byte{0x83, 0x01, 0x02, 0x03}}
	n.Pointers = []*Pointer{{KVs: []*KV{{Key: "foo", Value: d}}}}

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

func TestBasicBytesLoading(t *testing.T) {
	b := []byte("cats and dogs are taking over")
	o, err := cbor.DumpObject(b)
	if err != nil {
		t.Fatal(err)
	}

	var out []byte
	if err := cbor.DecodeInto(o, &out); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out, b) {
		t.Fatal("bytes round trip failed")
	}
}

func TestIsSerializationError(t *testing.T) {
	serr := NewSerializationError(fmt.Errorf("i am a cat"))

	if !xerrors.Is(serr, &SerializationError{}) {
		t.Fatal("its not a serialization error?")
	}
}
