package main

import (
	cbg "github.com/whyrusleeping/cbor-gen"

	hamt "github.com/ipfs/go-hamt-ipld"
)

func main() {
	// FIXME this will not generate the correct code, leave the cbor_gen.go file untouched.
	if err := cbg.WriteMapEncodersToFile("cbor_gen.go", "hamt", hamt.Node{}, hamt.KV{}); err != nil {
		panic(err)
	}

}
