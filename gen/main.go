package main

import (
	"os"

	cbg "github.com/whyrusleeping/cbor-gen"

	hamt "github.com/ipfs/go-hamt-ipld"
)

func main() {
	fi, err := os.Create("cbor_gen.go")
	if err != nil {
		panic(err)
	}
	defer fi.Close()

	if err := cbg.PrintHeaderAndUtilityMethods(fi, "hamt"); err != nil {
		panic(err)
	}

	t := []interface{}{
		hamt.Node{},
		hamt.KV{},
		hamt.Pointer{},
	}

	for _, t := range t {
		if err := cbg.GenTupleEncodersForType(t, fi); err != nil {
			panic(err)
		}
	}
}
