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
		out, ok := n.Find(k)
		if !ok {
			t.Fatal("should have found the thing")
		}
		if out != v {
			t.Fatal("got wrong value")
		}
	}
}
