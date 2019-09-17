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
	b.Run("find-10k", doBenchmarkEntriesCount(10000, 8))
	b.Run("find-100k", doBenchmarkEntriesCount(100000, 8))
	b.Run("find-1m", doBenchmarkEntriesCount(1000000, 8))
	b.Run("find-10k-bitwidth-6", doBenchmarkEntriesCount(10000, 6))
	b.Run("find-100k-bitwidth-6", doBenchmarkEntriesCount(100000, 6))
	b.Run("find-1m-bitwidth-6", doBenchmarkEntriesCount(1000000, 6))
	b.Run("find-10k-bitwidth-5", doBenchmarkEntriesCount(10000, 5))
	b.Run("find-100k-bitwidth-5", doBenchmarkEntriesCount(100000, 5))
	b.Run("find-1m-bitwidth-5", doBenchmarkEntriesCount(1000000, 5))
	b.Run("find-10k-bitwidth-4", doBenchmarkEntriesCount(10000, 4))
	b.Run("find-100k-bitwidth-4", doBenchmarkEntriesCount(100000, 4))
	b.Run("find-1m-bitwidth-4", doBenchmarkEntriesCount(1000000, 4))
	b.Run("find-10k-bitwidth-3", doBenchmarkEntriesCount(10000, 3))
	b.Run("find-100k-bitwidth-3", doBenchmarkEntriesCount(100000, 3))
	b.Run("find-1m-bitwidth-3", doBenchmarkEntriesCount(1000000, 3))
}

func doBenchmarkEntriesCount(num int, bitWidth int) func(b *testing.B) {
	r := rander{rand.New(rand.NewSource(int64(num)))}
	return func(b *testing.B) {
		cs := NewCborStore()
		n := NewNode(cs, UseTreeBitWidth(bitWidth))

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
			nd, err := LoadNode(context.TODO(), cs, c, UseTreeBitWidth(bitWidth))
			if err != nil {
				b.Fatal(err)
			}

			if err = nd.Find(context.TODO(), keys[i%num], nil); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	b.Run("write-1k", doBenchmarkWritesCount(1000, 8))
	b.Run("write-10k", doBenchmarkWritesCount(10000, 8))
	b.Run("write-100k", doBenchmarkWritesCount(100000, 8))
	b.Run("write-1k-bitwidth-6", doBenchmarkWritesCount(1000, 6))
	b.Run("write-10k-bitwidth-6", doBenchmarkWritesCount(10000, 6))
	b.Run("write-100k-bitwidth-6", doBenchmarkWritesCount(100000, 6))
	b.Run("write-1k-bitwidth-5", doBenchmarkWritesCount(1000, 5))
	b.Run("write-10k-bitwidth-5", doBenchmarkWritesCount(10000, 5))
	b.Run("write-100k-bitwidth-5", doBenchmarkWritesCount(100000, 5))
	b.Run("write-1k-bitwidth-4", doBenchmarkWritesCount(1000, 4))
	b.Run("write-10k-bitwidth-4", doBenchmarkWritesCount(10000, 4))
	b.Run("write-100k-bitwidth-4", doBenchmarkWritesCount(100000, 4))
	b.Run("write-1k-bitwidth-3", doBenchmarkWritesCount(1000, 3))
	b.Run("write-10k-bitwidth-3", doBenchmarkWritesCount(10000, 3))
	b.Run("write-100k-bitwidth-3", doBenchmarkWritesCount(100000, 3))
}

func doBenchmarkWritesCount(num int, bitWidth int) func(b *testing.B) {
	r := rander{rand.New(rand.NewSource(int64(num)))}
	getSize := func(bs *mockBlocks) int {
		sum := 0
		for _, val := range bs.data {
			sum += len(val)
		}
		return sum
	}

	return func(b *testing.B) {
		mb := newMockBlocks()
		cs := &CborIpldStore{Blocks: mb}
		n := NewNode(cs, UseTreeBitWidth(bitWidth))
		startSize := getSize(mb)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {

			for j := 0; j < num; j++ {
				k := r.randString()
				if err := n.Set(context.TODO(), k, r.randValue()); err != nil {
					b.Fatal(err)
				}
				if err := n.Flush(context.TODO()); err != nil {
					b.Fatal(err)
				}

				_, err := cs.Put(context.TODO(), n)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
		endSize := getSize(mb)
		b.Logf("%d Block Bytes Written/op", (endSize-startSize)/b.N)
	}
}
