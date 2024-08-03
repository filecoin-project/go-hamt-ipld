package main

import (
	"io"

	cbg "github.com/whyrusleeping/cbor-gen"

	hamt "github.com/filecoin-project/go-hamt-ipld/v4"
)

func main() {
	// FIXME this will not generate the correct code, leave the cbor_gen.go file untouched.
	if err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "hamt", hamt.Node[*dummy]{}, hamt.KV[*dummy]{}); err != nil {
		panic(err)
	}

}

type dummy int64

func (d dummy) New() *dummy                    { return nil }
func (d *dummy) Equals(*dummy) bool            { return false }
func (d dummy) MarshalCBOR(io.Writer) error    { return nil }
func (d *dummy) UnmarshalCBOR(io.Reader) error { return nil }
