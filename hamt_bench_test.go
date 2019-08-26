package hamt

import (
	"context"
	"encoding/hex"
	"math/rand"
	"runtime"
	"testing"
)

type rander struct {
	r *rand.Rand
}

func (r *rander) randString() string {
	buf := make([]byte, 18)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func (r *rander) randValue() []byte {
	buf := make([]byte, 30)
	rand.Read(buf)
	return buf
}

func BenchmarkSerializeNode(b *testing.B) {
	r := rander{rand.New(rand.NewSource(1234))}

	cs := NewCborStore()
	n := NewNode(cs)

	for i := 0; i < 50; i++ {
		if err := n.Set(context.TODO(), r.randString(), r.randValue()); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := cs.Put(context.TODO(), n)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFind(b *testing.B) {
	b.Run("find-10k", doBenchmarkEntriesCount(10000))
	b.Run("find-100k", doBenchmarkEntriesCount(100000))
	b.Run("find-1m", doBenchmarkEntriesCount(1000000))
}

func doBenchmarkEntriesCount(num int) func(b *testing.B) {
	r := rander{rand.New(rand.NewSource(int64(num)))}
	return func(b *testing.B) {
		cs := NewCborStore()
		n := NewNode(cs)

		var keys []string
		for i := 0; i < num; i++ {
			k := r.randString()
			if err := n.Set(context.TODO(), k, r.randValue()); err != nil {
				b.Fatal(err)
			}
			keys = append(keys, k)
		}

		if err := n.Flush(context.TODO()); err != nil {
			b.Fatal(err)
		}

		c, err := cs.Put(context.TODO(), n)
		if err != nil {
			b.Fatal(err)
		}

		runtime.GC()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			nd, err := LoadNode(context.TODO(), cs, c)
			if err != nil {
				b.Fatal(err)
			}

			_, err = nd.Find(context.TODO(), keys[i%num])
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
