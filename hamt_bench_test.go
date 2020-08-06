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
	kcount   int
	bitwidth int
}

var benchSetCaseTable []benchSetCase

func init() {
	kCounts := []int{
		1,
		10,
		100,
		1000,  // aka 1M
		10000, // aka 10M -- you'll need a lot of RAM for this.  Also, some patience.
	}
	bitwidths := []int{
		3,
		//4,
		5,
		//6,
		//7,
		8,
	}
	// bucketsize-aka-arraywidth?  maybe someday.
	for _, c := range kCounts {
		for _, bw := range bitwidths {
			benchSetCaseTable = append(benchSetCaseTable, benchSetCase{kcount: c, bitwidth: bw})
		}
	}
}

// BenchmarkFill creates a large HAMT, and measures how long it takes to generate all of this many entries.
//
// The number of blocks saved to the blockstore per entry is reported, and the total content size in bytes.
// The nanoseconds-per-op report on this function is not very useful, because the size of "op" varies with "n" between benchmarks.
//
// See "BenchmarkSet" for a probe of how long it takes to set additional entries in an already-large hamt
// (this gives a more interesting and useful nanoseconds-per-op).
func BenchmarkFill(b *testing.B) {
	for _, t := range benchSetCaseTable {
		b.Run(fmt.Sprintf("n=%dk/bitwidth=%d", t.kcount, t.bitwidth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := rander{rand.New(rand.NewSource(int64(i)))}
				blockstore := newMockBlocks()
				n := NewNode(cbor.NewCborStore(blockstore), UseTreeBitWidth(t.bitwidth))
				//b.ResetTimer()
				for j := 0; j < t.kcount*1000; j++ {
					if err := n.Set(context.Background(), r.randString(), r.randValue()); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(blockstore.data))/float64(t.kcount*1000), "blocks/entry")
				binarySize, _ := n.checkSize(context.Background())
				b.ReportMetric(float64(binarySize)/float64(t.kcount*1000), "bytes/entry")
				b.StartTimer()
			}
		})
	}
}

// BenchmarkSet creates a large HAMT, then resets the timer, and does another 1000 inserts,
// measuring the time taken for this second batch of inserts.
//
// The number of *additional* blocks per entry is reported.
func BenchmarkSet(b *testing.B) {
	for _, t := range benchSetCaseTable {
		b.Run(fmt.Sprintf("n=%dk/bitwidth=%d", t.kcount, t.bitwidth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := rander{rand.New(rand.NewSource(int64(i)))}
				blockstore := newMockBlocks()
				n := NewNode(cbor.NewCborStore(blockstore), UseTreeBitWidth(t.bitwidth))
				// Initial fill:
				for j := 0; j < t.kcount*1000; j++ {
					if err := n.Set(context.Background(), r.randString(), r.randValue()); err != nil {
						b.Fatal(err)
					}
				}
				initalBlockstoreSize := len(blockstore.data)
				b.ResetTimer()
				// Additional inserts:
				b.ReportAllocs()
				for j := 0; j < 1000; j++ {
					if err := n.Set(context.Background(), r.randString(), r.randValue()); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(blockstore.data)-initalBlockstoreSize)/float64(1000), "addntlBlocks/addntlEntry")
			}
		})
	}
}

func BenchmarkFind(b *testing.B) {
	for _, t := range benchSetCaseTable {
		b.Run(fmt.Sprintf("n=%dk/bitwidth=%d", t.kcount, t.bitwidth),
			doBenchmarkEntriesCount(t.kcount*1000, t.bitwidth))
	}
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
