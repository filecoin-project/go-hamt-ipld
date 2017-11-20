package hamt

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func randString() string {
	buf := make([]byte, 8)
	rand.Read(buf)
	return hex.EncodeToString(buf)
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
		*name++
		if p.isShard() {
			fmt.Printf("\tn%d -> n%d;\n", cur, *name)
			dotGraphRec(p.Link, name)
		} else {
			var names []string
			for _, pt := range p.KVs {
				names = append(names, pt.Key)
			}
			fmt.Printf("\tn%d -> n%s;\n", cur, strings.Join(names, "-"))
		}
	}
}

func TestSetGet(t *testing.T) {
	ctx := context.Background()
	vals := make(map[string]string)
	var keys []string
	for i := 0; i < 100000; i++ {
		s := randString()
		vals[s] = randString()
		keys = append(keys, s)
	}

	n := NewNode()
	for _, k := range keys {
		n.Set(ctx, k, vals[k])
	}

	for k, v := range vals {
		out, err := n.Find(ctx, k)
		if err != nil {
			t.Fatal("should have found the thing")
		}
		if out != v {
			t.Fatal("got wrong value")
		}
	}

	for i := 0; i < 100; i++ {
		_, err := n.Find(ctx, randString())
		if err != ErrNotFound {
			t.Fatal("should have gotten ErrNotFound, instead got: ", err)
		}
	}

	for k := range vals {
		next := randString()
		n.Set(ctx, k, next)
		vals[k] = next
	}

	for k, v := range vals {
		out, err := n.Find(ctx, k)
		if err != nil {
			t.Fatal("should have found the thing")
		}
		if out != v {
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
