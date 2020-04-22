package hamt

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	cbor "github.com/ipfs/go-ipld-cbor"
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

	cs := cbor.NewCborStore(newMockBlocks())
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

type benchSetCase struct {
	count    int
	bitwidth int
}

func BenchmarkSet(b *testing.B) {
	kCounts := []int{1, 10, 100}
	bitwidths := []int{5, 8}

	var table []benchSetCase
	for _, c := range kCounts {

		for _, bw := range bitwidths {
			table = append(table, benchSetCase{count: c * 1000, bitwidth: bw})
		}

	}
	r := rander{rand.New(rand.NewSource(int64(42)))}
	for _, t := range table {
		b.Run(fmt.Sprintf("%d/%d", t.count, t.bitwidth), func(b *testing.B) {
			ctx := context.Background()
			n := NewNode(cbor.NewCborStore(newMockBlocks()), UseTreeBitWidth(t.bitwidth))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < t.count; j++ {
					if err := n.Set(ctx, r.randString(), r.randValue()); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkFind(b *testing.B) {
	b.Run("find-10k", doBenchmarkEntriesCount(10000, 8))
	b.Run("find-100k", doBenchmarkEntriesCount(100000, 8))
	b.Run("find-1m", doBenchmarkEntriesCount(1000000, 8))
	b.Run("find-10k-bitwidth-5", doBenchmarkEntriesCount(10000, 5))
	b.Run("find-100k-bitwidth-5", doBenchmarkEntriesCount(100000, 5))
	b.Run("find-1m-bitwidth-5", doBenchmarkEntriesCount(1000000, 5))

}

func doBenchmarkEntriesCount(num int, bitWidth int) func(b *testing.B) {
	r := rander{rand.New(rand.NewSource(int64(num)))}
	return func(b *testing.B) {
		cs := cbor.NewCborStore(newMockBlocks())
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
