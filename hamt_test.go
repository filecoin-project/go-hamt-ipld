package hamt

import (
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

func verifyStructure(n *Node) error {
	for _, p := range n.Pointers {
		switch p := p.(type) {
		case *Pointer:
			switch ch := p.Obj.(type) {
			case string:
				continue
			case *Node:
				switch len(ch.Pointers) {
				case 0:
					return fmt.Errorf("node has child with no children")
				case 1:
					return fmt.Errorf("node has child with only a single child")
				default:
					if err := verifyStructure(ch); err != nil {
						return err
					}
				}
			default:
				panic("wrong type")
			}
		case []*Pointer:
			panic("NYI")
		}
	}
	return nil
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
		switch p := p.(type) {
		case *Pointer:
			if ch, ok := p.Obj.(*Node); ok {
				fmt.Printf("\tn%d -> n%d;\n", cur, *name)
				dotGraphRec(ch, name)
			} else {
				fmt.Printf("\tn%d -> n%s;\n", cur, p.Key)
			}
		case []*Pointer:
			var names []string
			for _, pt := range p {
				names = append(names, pt.Key)
			}
			fmt.Printf("\tn%d -> n%s;\n", cur, strings.Join(names, "-"))
		}
	}
}

func TestSetGet(t *testing.T) {
	rand.Seed(6)
	vals := make(map[string]string)
	var keys []string
	for i := 0; i < 100000; i++ {
		s := randString()
		vals[s] = randString()
		keys = append(keys, s)
	}

	n := NewNode()
	for _, k := range keys {
		n.Set(k, vals[k])
	}

	for k, v := range vals {
		out, err := n.Find(k)
		if err != nil {
			t.Fatal("should have found the thing")
		}
		if out != v {
			t.Fatal("got wrong value")
		}
	}

	for i := 0; i < 100; i++ {
		_, err := n.Find(randString())
		if err != ErrNotFound {
			t.Fatal("should have gotten ErrNotFound, instead got: ", err)
		}
	}

	for k := range vals {
		next := randString()
		n.Set(k, next)
		vals[k] = next
	}

	for k, v := range vals {
		out, err := n.Find(k)
		if err != nil {
			t.Fatal("should have found the thing")
		}
		if out != v {
			t.Fatal("got wrong value")
		}
	}

	/*
		for i := 0; i < 100; i++ {
			err := n.Delete(randString())
			if err != ErrNotFound {
				t.Fatal("should have gotten ErrNotFound, instead got: ", err)
			}
		}
	*/

	for _, k := range keys {
		if err := n.Delete(k); err != nil {
			fmt.Println(k)
			t.Fatal(err)
		}
		if _, err := n.Find(k); err != ErrNotFound {
			t.Fatal("Expected ErrNotFound, got: ", err)
		}
	}
}
