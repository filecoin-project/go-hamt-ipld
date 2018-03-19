// +build gofuzz

package hamt

import (
	"context"
	"encoding/binary"
	"fmt"
	mrand "math/rand"
	"strings"
)

func fuzzNodesEqual(store *CborIpldStore, n1, n2 *Node) bool {
	ctx := context.Background()
	err := n1.Flush(ctx)
	if err != nil {
		panic(err)
	}
	n1Cid, err := store.Put(ctx, n1)
	if err != nil {
		panic(err)
	}
	err = n2.Flush(ctx)
	if err != nil {
		panic(err)
	}
	n2Cid, err := store.Put(ctx, n2)
	if err != nil {
		panic(err)
	}
	return n1Cid.Equals(n2Cid)
}
func fuzzDotGraph(n *Node) {
	fmt.Println("digraph foo {")
	name := 0
	fuzzDotGraphRec(n, &name)
	fmt.Println("}")
}

func fuzzDotGraphRec(n *Node, name *int) {
	cur := *name
	for _, p := range n.Pointers {
		if p.isShard() {
			*name++
			fmt.Printf("\tn%d -> n%d;\n", cur, *name)
			nd, err := p.loadChild(context.Background(), n.store)
			if err != nil {
				panic(err)
			}

			fuzzDotGraphRec(nd, name)
		} else {
			var names []string
			for _, pt := range p.KVs {
				names = append(names, pt.Key)
			}
			fmt.Printf("\tn%d -> n%s;\n", cur, strings.Join(names, "_"))
		}
	}
}

func Fuzz(data []byte) int {
	if len(data) < 22+4 {
		return 0
	}
	rand := mrand.New(mrand.NewSource(int64(binary.LittleEndian.Uint64(data))))
	data = data[4:]
	randValue := func() []byte {
		buf := make([]byte, 30)
		rand.Read(buf)
		return buf
	}

	cs := NewCborStore()
	root := NewNode(cs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	masterMap := make(map[string][]byte)

	for len(data) >= 11 {
		add := data[0]%2 == 1
		data = data[1:]
		var key string
		key, data = string(data[:10]), data[10:]
		if add {
			val := randValue()
			err := root.Set(ctx, key, val)
			if err != nil {
				panic(err)
			}

			masterMap[key] = val
		} else {
			err := root.Delete(ctx, key)
			if err == ErrNotFound {
				_, found := masterMap[key]
				if found {
					panic("not found a key that should have been found")
				}
			} else if err != nil {
				panic(err)
			}

			delete(masterMap, key)
		}
	}

	root2 := NewNode(cs)
	for k, v := range masterMap {
		root2.Set(ctx, k, v)
	}
	if !fuzzNodesEqual(cs, root, root2) {
		fmt.Printf("Expected node:\n")
		fuzzDotGraph(root2)
		fmt.Printf("After transfomrations node:\n")
		fuzzDotGraph(root)
		panic("nodes not equal")
	}

	return 1
}
