package main

import (
	"io"

	cbg "github.com/whyrusleeping/cbor-gen"

	hamt "github.com/filecoin-project/go-hamt-ipld/v4"
)

func main() {
	if err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "hamt", hamt.Node[dummy]{}, hamt.KV[dummy]{}); err != nil {
		panic(err)
	}

}

// dummy generic type that cbor-gen will replace with T
type dummy int64

func (d dummy) New() dummy                    { return dummy(0) }
func (d dummy) Equals(dummy) bool             { return false }
func (d dummy) MarshalCBOR(io.Writer) error   { return nil }
func (d dummy) UnmarshalCBOR(io.Reader) error { return nil }
