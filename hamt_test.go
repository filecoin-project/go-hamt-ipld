package hamt

import (
	"encoding/hex"
	"math/rand"
	"testing"
)

func randString() string {
	buf := make([]byte, 8)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func TestSetGet(t *testing.T) {
	vals := make(map[string]string)
	for i := 0; i < 10000; i++ {
		vals[randString()] = randString()
	}

	n := NewNode()
	for k, v := range vals {
		n.Set(k, v)
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

	for i := 0; i < 100; i++ {
		err := n.Delete(randString())
		if err != ErrNotFound {
			t.Fatal("should have gotten ErrNotFound, instead got: ", err)
		}
	}

	for k := range vals {
		if err := n.Delete(k); err != nil {
			t.Fatal(err)
		}
		if _, err := n.Find(k); err != ErrNotFound {
			t.Fatal("Expected ErrNotFound, got: ", err)
		}
	}
}
