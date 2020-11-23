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

func (r *rander) randString(stringSize int) string {
	buf := make([]byte, stringSize)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func (r *rander) randValue(datasize int) []byte {
	buf := make([]byte, datasize)
	rand.Read(buf)
	return buf
}

func (r *rander) selectKey(keys []string) string {
	i := rand.Int() % len(keys)
	return keys[i]
}

func BenchmarkSerializeNode(b *testing.B) {
	r := rander{rand.New(rand.NewSource(1234))}

	cs := cbor.NewCborStore(newMockBlocks())
	n := NewNode(cs)

	for i := 0; i < 50; i++ {
		if err := n.Set(context.TODO(), r.randString(18), r.randValue(30)); err != nil {
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

type hamtParams struct {
	id       string
	count    int
	datasize int
	keysize  int
}

type benchCase struct {
	id       string
	count    int
	bitwidth int
	datasize int
	keysize  int
}

var caseTable []benchCase

func init() {

	bitwidths := []int{
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
	}

	hamts := []hamtParams{
		hamtParams{
			id:       "init.AddressMap",
			count:    55649,
			datasize: 3,
			keysize:  26,
		},
		hamtParams{
			id:       "market.PendingProposals",
			count:    40713,
			datasize: 151,
			keysize:  38,
		},
		hamtParams{
			id:       "market.EscrowWTable",
			count:    2113,
			datasize: 7,
			keysize:  4,
		},
		hamtParams{
			id:       "market.LockedTable",
			count:    2098,
			datasize: 4,
			keysize:  4,
		},
		hamtParams{
			id:       "market.DealOpsByEpoch",
			count:    16558,
			datasize: 43,
			keysize:  3,
		},
		hamtParams{
			id:       "power.CronEventQueue",
			count:    60,
			datasize: 43,
			keysize:  3,
		},
		hamtParams{
			id:       "power.CLaims",
			count:    15610,
			datasize: 5,
			keysize:  3,
		},
	}

	// bucketsize-aka-arraywidth?  maybe someday.
	for _, h := range hamts {
		for _, bw := range bitwidths {
			caseTable = append(caseTable,
				benchCase{
					id:       fmt.Sprintf("%s -- bw=%d", h.id, bw),
					count:    h.count,
					bitwidth: bw,
					datasize: h.datasize,
					keysize:  h.keysize,
				})
		}
	}
}

// The benchmark results can be graphed.  Here are some reasonable selections:
/*
	benchdraw --filter=BenchmarkFill          --plot=line --x=n "--y=blocks/entry"                 < sample > BenchmarkFill-blocks-per-entry-vs-scale.svg
	benchdraw --filter=BenchmarkFill          --plot=line --x=n "--y=bytes(blockstoreAccnt)/entry" < sample > BenchmarkFill-totalBytes-per-entry-vs-scale.svg
	benchdraw --filter=BenchmarkSetBulk       --plot=line --x=n "--y=addntlBlocks/addntlEntry"     < sample > BenchmarkSetBulk-addntlBlocks-per-addntlEntry-vs-scale.svg
	benchdraw --filter=BenchmarkSetIndividual --plot=line --x=n "--y=addntlBlocks/addntlEntry"     < sample > BenchmarkSetIndividual-addntlBlocks-per-addntlEntry-vs-scale.svg
	benchdraw --filter=BenchmarkFind          --plot=line --x=n "--y=ns/op"                        < sample > BenchmarkFind-speed-vs-scale.svg
	benchdraw --filter=BenchmarkFind          --plot=line --x=n "--y=getEvts/find"                 < sample > BenchmarkFind-getEvts-vs-scale.svg
*/
// (The 'benchdraw' command alluded to here is https://github.com/cep21/benchdraw .)

// Histograms of blocksizes can be logged from some of the following functions, but are commented out.
// The main thing to check for in those is whether there are any exceptionally small blocks being produced:
// less than 64 bytes is a bit concerning because we assume there's some overhead per block in most operations (even if the exact amount may vary situationally).
// We do see some of these small blocks with small bitwidth parameters (e.g. 3), but almost none with larger bitwidth parameters.

// BenchmarkFill creates a large HAMT, and measures how long it takes to generate all of this many entries;
// the number of entries is varied in sub-benchmarks, denoted by their "n=" label component.
// Flush is done once for the entire structure, meaning the number of blocks generated per entry can be much fewer than 1.
//
// The number of blocks saved to the blockstore per entry is reported, and the total content size in bytes.
// The nanoseconds-per-op report on this function is not very useful, because the size of "op" varies with "n" between benchmarks.
//
// See "BenchmarkSet*" for a probe of how long it takes to set additional entries in an already-large hamt
// (this gives a more interesting and useful nanoseconds-per-op indicators).
func BenchmarkFill(b *testing.B) {
	for _, t := range caseTable {
		b.Run(fmt.Sprintf("%s", t.id), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := rander{rand.New(rand.NewSource(int64(i)))}
				blockstore := newMockBlocks()
				n := NewNode(cbor.NewCborStore(blockstore), UseTreeBitWidth(t.bitwidth))
				//b.ResetTimer()
				for j := 0; j < t.count; j++ {
					if err := n.Set(context.Background(), r.randString(t.keysize), r.randValue(t.datasize)); err != nil {
						b.Fatal(err)
					}
				}
				if err := n.Flush(context.Background()); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if i < 3 {
					//b.Logf("block size histogram: %v\n", blockstore.getBlockSizesHistogram())
				}
				if blockstore.stats.evtcntPutDup > 0 {
					b.Logf("on round N=%d: blockstore stats: %#v\n", b.N, blockstore.stats) // note: must refer to this before doing `n.checkSize`; that function has many effects.
				}
				b.ReportMetric(float64(blockstore.stats.evtcntGet)/float64(t.count), "getEvts")
				b.ReportMetric(float64(blockstore.stats.evtcntPut)/float64(t.count), "putEvts")
				b.ReportMetric(float64(len(blockstore.data))/float64(t.count), "blocks")
				binarySize, _ := n.checkSize(context.Background())
				b.ReportMetric(float64(binarySize)/float64(t.count), "bytes(hamtAccnt)/entry")
				b.ReportMetric(float64(blockstore.totalBlockSizes())/float64(t.count), "bytes(blockstoreAccnt)/entry")
				b.StartTimer()
			}
		})
	}
}

// BenchmarkSetBulk creates a large HAMT, then starts the timer, and does another 1000 inserts,
// measuring the time taken for this second batch of inserts.
// Flushing happens once after all 1000 inserts.
//
// The number of *additional* blocks per entry is reported.
// This number is usually less than one with high flush interval means changes might be amortized.
func BenchmarkSetBulk(b *testing.B) {
	doBenchmarkSetSuite(b, false)
}

// BenchmarkSetIndividual is the same as BenchmarkSetBulk, but flushes more.
// Flush happens per insert.
//
// The number of *additional* blocks per entry is reported.
// Since we flush each insert individually, this number should be at least 1.
func BenchmarkSetIndividual(b *testing.B) {
	doBenchmarkSetSuite(b, true)
}

func doBenchmarkSetSuite(b *testing.B, flushPer bool) {
	for _, t := range caseTable {
		b.Run(fmt.Sprintf("%s", t.id), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r := rander{rand.New(rand.NewSource(int64(i)))}
				blockstore := newMockBlocks()
				n := NewNode(cbor.NewCborStore(blockstore), UseTreeBitWidth(t.bitwidth))
				// Initial fill:
				for j := 0; j < t.count; j++ {
					if err := n.Set(context.Background(), r.randString(t.keysize), r.randValue(t.datasize)); err != nil {
						b.Fatal(err)
					}
				}
				if err := n.Flush(context.Background()); err != nil {
					b.Fatal(err)
				}
				//	b.ResetTimer()
				blockstore.stats = blockstoreStats{}
				// Additional inserts:
				b.ReportAllocs()
				b.StartTimer()
				for j := 0; j < 1000; j++ {
					if err := n.Set(context.Background(), r.randString(t.keysize), r.randValue(t.datasize)); err != nil {
						b.Fatal(err)
					}
					if flushPer {
						if err := n.Flush(context.Background()); err != nil {
							b.Fatal(err)
						}
					}
				}
				if !flushPer {
					if err := n.Flush(context.Background()); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if i < 3 {
					// b.Logf("block size histogram: %v\n", blockstore.getBlockSizesHistogram())
				}
				if blockstore.stats.evtcntPutDup > 0 {
					b.Logf("on round N=%d: blockstore stats: %#v\n", b.N, blockstore.stats)
				}
				b.ReportMetric(float64(blockstore.stats.evtcntGet)/1000, "getEvts")
				b.ReportMetric(float64(blockstore.stats.evtcntPut)/1000, "putEvts")
				b.ReportMetric(float64(blockstore.stats.bytesPut)/1000, "bytesPut")
				b.StartTimer()
			}
		})
	}
}

func BenchmarkFind(b *testing.B) {
	for _, t := range caseTable {
		b.Run(fmt.Sprintf("%s", t.id),
			doBenchmarkEntriesCount(t.count, t.bitwidth, t.datasize, t.keysize))
	}
}

func doBenchmarkEntriesCount(num int, bitWidth int, datasize int, keysize int) func(b *testing.B) {
	r := rander{rand.New(rand.NewSource(int64(num)))}
	return func(b *testing.B) {
		blockstore := newMockBlocks()
		cs := cbor.NewCborStore(blockstore)
		n := NewNode(cs, UseTreeBitWidth(bitWidth))

		var keys []string
		for i := 0; i < num; i++ {
			k := r.randString(keysize)
			if err := n.Set(context.TODO(), k, r.randValue(datasize)); err != nil {
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
			blockstore.stats = blockstoreStats{}
			for j := 0; j < 1000; j++ {
				nd, err := LoadNode(context.TODO(), cs, c, UseTreeBitWidth(bitWidth))
				if err != nil {
					b.Fatal(err)
				}
				if err = nd.Find(context.TODO(), r.selectKey(keys), nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(blockstore.stats.evtcntGet)/float64(1000), "getEvts")
			b.ReportMetric(float64(blockstore.stats.evtcntPut)/float64(1000), "putEvts") // surely this is zero, but for completeness.
			b.StartTimer()
		}
	}
}

func BenchmarkReset(b *testing.B) {
	for _, t := range caseTable {
		b.Run(fmt.Sprintf("%s", t.id),
			doBenchmarkResetSuite(t.count, t.bitwidth, t.datasize, t.keysize))
	}
}

func doBenchmarkResetSuite(num int, bitWidth int, datasize int, keysize int) func(b *testing.B) {
	r := rander{rand.New(rand.NewSource(int64(num)))}
	return func(b *testing.B) {
		blockstore := newMockBlocks()
		cs := cbor.NewCborStore(blockstore)
		n := NewNode(cs, UseTreeBitWidth(bitWidth))

		var keys []string
		for i := 0; i < num; i++ {
			k := r.randString(keysize)
			if err := n.Set(context.TODO(), k, r.randValue(datasize)); err != nil {
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
			blockstore.stats = blockstoreStats{}
			for j := 0; j < 1000; j++ {
				nd, err := LoadNode(context.TODO(), cs, c, UseTreeBitWidth(bitWidth))
				if err != nil {
					b.Fatal(err)
				}
				if err := nd.Set(context.Background(), r.selectKey(keys), r.randValue(datasize)); err != nil {
					b.Fatal(err)
				}
				if err := nd.Flush(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(blockstore.stats.evtcntGet)/1000, "getEvts")
			b.ReportMetric(float64(blockstore.stats.evtcntPut)/1000, "putEvts")
			b.ReportMetric(float64(blockstore.stats.bytesPut)/1000, "bytesPut")
			b.StartTimer()
		}
	}
}
