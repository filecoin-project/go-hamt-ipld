package main

import (
	cbg "github.com/whyrusleeping/cbor-gen"

	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
)

func main() {
	// FIXME this will not generate the correct code, leave the cbor_gen.go file untouched.
	if err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "hamt", hamt.Node{}, hamt.KV{}); err != nil {
		panic(err)
	}

}
