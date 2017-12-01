package hamt

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func randString() string {
	buf := make([]byte, 18)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func randValue() []byte {
	buf := make([]byte, 30)
	rand.Read(buf)
	return buf
}

func dotGraph(n *Node) {
	fmt.Println("digraph foo {")
	name := 0
	dotGraphRec(n, &name)
	fmt.Println("}")
}

func dotGraphRec(n *Node, name *int) {
	cur := *name
	for _, p := range n.Pointers {
		if p.isShard() {
			*name++
			fmt.Printf("\tn%d -> n%d;\n", cur, *name)
			nd, err := p.loadChild(context.Background(), n.store)
			if err != nil {
				panic(err)
			}

			dotGraphRec(nd, name)
		} else {
			var names []string
			for _, pt := range p.KVs {
				names = append(names, pt.Key)
			}
			fmt.Printf("\tn%d -> n%s;\n", cur, strings.Join(names, "_"))
		}
	}
}

func stats(n *Node) (int, int, int, int) {
	var totalnodes, totalkvs, pairs, triples int
	totalnodes = 1
	for _, p := range n.Pointers {
		if p.isShard() {
			nd, err := p.loadChild(context.Background(), n.store)
			if err != nil {
				panic(err)
			}

			t, k, two, three := stats(nd)
			totalnodes += t
			totalkvs += k
			pairs += two
			triples += three
		} else {
			totalkvs += len(p.KVs)
			switch len(p.KVs) {
			case 2:
				pairs++
			case 3:
				triples++
			}
		}
	}
	return totalnodes, totalkvs, pairs, triples
}

func TestSetGet(t *testing.T) {
	ctx := context.Background()
	vals := make(map[string][]byte)
	var keys []string
	for i := 0; i < 100000; i++ {
		s := randString()
		vals[s] = randValue()
		keys = append(keys, s)
	}

	cs := NewCborStore()
	begn := NewNode(cs)
	for _, k := range keys {
		begn.Set(ctx, k, vals[k])
	}

	size, err := begn.checkSize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mapsize := 0
	for k, v := range vals {
		mapsize += (len(k) + len(v))
	}
	fmt.Printf("Total size is: %d, size of keys+vals: %d, overhead: %.2f\n", size, mapsize, float64(size)/float64(mapsize))
	fmt.Println(stats(begn))

	fmt.Println("start flush")
	bef := time.Now()
	if err := begn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("flush took: ", time.Since(bef), puts)
	c, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}

	var n Node
	if err := cs.Get(ctx, c, &n); err != nil {
		t.Fatal(err)
	}
	n.store = cs

	bef = time.Now()
	for k, v := range vals {
		out, err := n.Find(ctx, k)
		if err != nil {
			t.Fatal("should have found the thing: ", err)
		}
		if !bytes.Equal(out, v) {
			t.Fatal("got wrong value")
		}
	}
	fmt.Println("finds took: ", time.Since(bef))

	for i := 0; i < 100; i++ {
		_, err := n.Find(ctx, randString())
		if err != ErrNotFound {
			t.Fatal("should have gotten ErrNotFound, instead got: ", err)
		}
	}

	for k := range vals {
		next := randValue()
		n.Set(ctx, k, next)
		vals[k] = next
	}

	for k, v := range vals {
		out, err := n.Find(ctx, k)
		if err != nil {
			t.Fatal("should have found the thing")
		}
		if !bytes.Equal(out, v) {
			t.Fatal("got wrong value after value change")
		}
	}

	for i := 0; i < 100; i++ {
		err := n.Delete(ctx, randString())
		if err != ErrNotFound {
			t.Fatal("should have gotten ErrNotFound, instead got: ", err)
		}
	}

	for _, k := range keys {
		if err := n.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
		if _, err := n.Find(ctx, k); err != ErrNotFound {
			t.Fatal("Expected ErrNotFound, got: ", err)
		}
	}
}
