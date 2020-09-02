package hamt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	block "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type mockBlocks struct {
	data  map[cid.Cid]block.Block
	stats blockstoreStats
}

func newMockBlocks() *mockBlocks {
	return &mockBlocks{make(map[cid.Cid]block.Block), blockstoreStats{}}
}

func (mb *mockBlocks) Get(c cid.Cid) (block.Block, error) {
	mb.stats.evtcntGet++
	d, ok := mb.data[c]
	if ok {
		return d, nil
	}
	return nil, fmt.Errorf("Not Found")
}

func (mb *mockBlocks) Put(b block.Block) error {
	mb.stats.evtcntPut++
	if _, exists := mb.data[b.Cid()]; exists {
		mb.stats.evtcntPutDup++
	}
	mb.data[b.Cid()] = b
	return nil
}

type blockstoreStats struct {
	evtcntGet    int
	evtcntPut    int
	evtcntPutDup int
}

func (mb *mockBlocks) totalBlockSizes() int {
	sum := 0
	for _, v := range mb.data {
		sum += len(v.RawData())
	}
	return sum
}

type blockSizesHistogram [12]int

func (mb *mockBlocks) getBlockSizesHistogram() (h blockSizesHistogram) {
	for _, v := range mb.data {
		l := len(v.RawData())
		switch {
		case l <= 2<<2: // 8
			h[0]++
		case l <= 2<<3: // 16
			h[1]++
		case l <= 2<<4: // 32
			h[2]++
		case l <= 2<<5: // 64
			h[3]++
		case l <= 2<<6: // 128
			h[4]++
		case l <= 2<<7: // 256
			h[5]++
		case l <= 2<<8: // 512
			h[6]++
		case l <= 2<<9: // 1024
			h[7]++
		case l <= 2<<10: // 2048
			h[8]++
		case l <= 2<<11: // 4096
			h[9]++
		case l <= 2<<12: // 8192
			h[10]++
		default:
			h[11]++
		}
	}
	return
}

func (h blockSizesHistogram) String() string {
	v := "["
	v += "<=" + strconv.Itoa(2<<2) + ":" + strconv.Itoa(h[0]) + ", "
	v += "<=" + strconv.Itoa(2<<3) + ":" + strconv.Itoa(h[1]) + ", "
	v += "<=" + strconv.Itoa(2<<4) + ":" + strconv.Itoa(h[2]) + ", "
	v += "<=" + strconv.Itoa(2<<5) + ":" + strconv.Itoa(h[3]) + ", "
	v += "<=" + strconv.Itoa(2<<6) + ":" + strconv.Itoa(h[4]) + ", "
	v += "<=" + strconv.Itoa(2<<7) + ":" + strconv.Itoa(h[5]) + ", "
	v += "<=" + strconv.Itoa(2<<8) + ":" + strconv.Itoa(h[6]) + ", "
	v += "<=" + strconv.Itoa(2<<9) + ":" + strconv.Itoa(h[7]) + ", "
	v += "<=" + strconv.Itoa(2<<10) + ":" + strconv.Itoa(h[8]) + ", "
	v += "<=" + strconv.Itoa(2<<11) + ":" + strconv.Itoa(h[9]) + ", "
	v += "<=" + strconv.Itoa(2<<12) + ":" + strconv.Itoa(h[10]) + ", "
	v += ">" + strconv.Itoa(2<<12) + ":" + strconv.Itoa(h[11])
	return v + "]"
}

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

var identityHash = func(k []byte) []byte {
	res := make([]byte, 32)
	copy(res, k)
	return res
}

var shortIdentityHash = func(k []byte) []byte {
	res := make([]byte, 16)
	copy(res, k)
	return res
}

func TestCanonicalStructure(t *testing.T) {
	addAndRemoveKeys(t, []string{"K"}, []string{"B"}, UseHashFunction(identityHash))
	addAndRemoveKeys(t, []string{"K0", "K1", "KAA1", "KAA2", "KAA3"}, []string{"KAA4"})
}

func TestCanonicalStructureAlternateBitWidth(t *testing.T) {
	addAndRemoveKeys(t, []string{"K"}, []string{"B"}, UseTreeBitWidth(7), UseHashFunction(identityHash))
	addAndRemoveKeys(t, []string{"K0", "K1", "KAA1", "KAA2", "KAA3"}, []string{"KAA4"}, UseTreeBitWidth(7), UseHashFunction(identityHash))
	addAndRemoveKeys(t, []string{"K"}, []string{"B"}, UseTreeBitWidth(6), UseHashFunction(identityHash))
	addAndRemoveKeys(t, []string{"K0", "K1", "KAA1", "KAA2", "KAA3"}, []string{"KAA4"}, UseTreeBitWidth(6), UseHashFunction(identityHash))
	addAndRemoveKeys(t, []string{"K"}, []string{"B"}, UseTreeBitWidth(5), UseHashFunction(identityHash))
	addAndRemoveKeys(t, []string{"K0", "K1", "KAA1", "KAA2", "KAA3"}, []string{"KAA4"}, UseTreeBitWidth(5), UseHashFunction(identityHash))
}

func TestOverflow(t *testing.T) {
	keys := make([]string, 4)
	for i := range keys {
		keys[i] = strings.Repeat("A", 32) + fmt.Sprintf("%d", i)
	}

	cs := cbor.NewCborStore(newMockBlocks())
	n := NewNode(cs, UseHashFunction(identityHash))
	for _, k := range keys[:3] {
		if err := n.Set(context.Background(), k, "foobar"); err != nil {
			t.Error(err)
		}
	}

	// Try forcing the depth beyond 32
	if err := n.Set(context.Background(), keys[3], "bad"); err != ErrMaxDepth {
		t.Errorf("expected error %q, got %q", ErrMaxDepth, err)
	}

	// Force _to_ max depth.
	if err := n.Set(context.Background(), keys[3][1:], "bad"); err != nil {
		t.Error(err)
	}

	// Now, try fetching with a shorter hash function.
	n.hash = shortIdentityHash
	if err := n.Find(context.Background(), keys[0], nil); err != ErrMaxDepth {
		t.Errorf("expected error %q, got %q", ErrMaxDepth, err)
	}
}

func TestFillAndCollapse(t *testing.T) {
	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())
	root := NewNode(cs, UseTreeBitWidth(8), UseHashFunction(identityHash))
	val := randValue()

	// start with a single node and a single full bucket
	if err := root.Set(ctx, "AAAAAA11", val); err != nil {
		t.Fatal(err)
	}
	if err := root.Set(ctx, "AAAAAA12", val); err != nil {
		t.Fatal(err)
	}
	if err := root.Set(ctx, "AAAAAA21", val); err != nil {
		t.Fatal(err)
	}

	st := stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 1 || st.totalKvs != 3 || st.counts[3] != 1 {
		t.Fatal("Should be 1 node with 1 bucket")
	}

	baseCid, err := cs.Put(ctx, root)
	if err != nil {
		t.Fatal(err)
	}

	// add a 4th colliding entry that forces a chain of new nodes to accommodate
	// in a new node where there aren't collisions (7th byte)
	if err := root.Set(ctx, "AAAAAA22", val); err != nil {
		t.Fatal(err)
	}

	st = stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 7 || st.totalKvs != 4 || st.counts[2] != 2 {
		t.Fatal("Should be 7 nodes with 4 buckets")
	}

	// remove and we should be back to the same structure as before
	if err := root.Delete(ctx, "AAAAAA22"); err != nil {
		t.Fatal(err)
	}

	st = stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 1 || st.totalKvs != 3 || st.counts[3] != 1 {
		t.Fatal("Should be 1 node with 1 bucket")
	}

	c, err := cs.Put(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Equals(baseCid) {
		t.Fatal("CID mismatch on mutation")
	}

	// insert elements that collide at the 4th position so push the tree down by
	// 3 nodes
	if err := root.Set(ctx, "AAA11AA", val); err != nil {
		t.Fatal(err)
	}
	if err := root.Set(ctx, "AAA12AA", val); err != nil {
		t.Fatal(err)
	}
	if err := root.Set(ctx, "AAA13AA", val); err != nil {
		t.Fatal(err)
	}
	st = stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 4 || st.totalKvs != 6 || st.counts[3] != 2 {
		t.Fatal("Should be 4 nodes with 2 buckets of 3")
	}

	midCid, err := cs.Put(ctx, root)
	if err != nil {
		t.Fatal(err)
	}

	// insert an overflow node that pushes the previous 4 into a separate node
	if err := root.Set(ctx, "AAA14AA", val); err != nil {
		t.Fatal(err)
	}

	st = stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 5 || st.totalKvs != 7 || st.counts[1] != 4 || st.counts[3] != 1 {
		t.Fatal("Should be 4 node with 2 buckets")
	}

	// put the colliding 4th back in that will push down to full height
	if err := root.Set(ctx, "AAAAAA22", val); err != nil {
		t.Fatal(err)
	}

	st = stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 8 || st.totalKvs != 8 || st.counts[1] != 4 || st.counts[2] != 2 {
		t.Fatal("Should be 7 nodes with 5 buckets")
	}

	// rewind back one step
	if err := root.Delete(ctx, "AAAAAA22"); err != nil {
		t.Fatal(err)
	}

	st = stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 5 || st.totalKvs != 7 || st.counts[1] != 4 || st.counts[3] != 1 {
		t.Fatal("Should be 4 node with 2 buckets")
	}

	// rewind another step
	if err := root.Delete(ctx, "AAA14AA"); err != nil {
		t.Fatal(err)
	}
	st = stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 4 || st.totalKvs != 6 || st.counts[3] != 2 {
		t.Fatal("Should be 4 nodes with 2 buckets of 3")
	}

	c, err = cs.Put(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Equals(midCid) {
		t.Fatal("CID mismatch on mutation")
	}

	// remove the 3 colliding node so we should be back to the initial state
	if err := root.Delete(ctx, "AAA11AA"); err != nil {
		t.Fatal(err)
	}
	if err := root.Delete(ctx, "AAA12AA"); err != nil {
		t.Fatal(err)
	}
	if err := root.Delete(ctx, "AAA13AA"); err != nil {
		t.Fatal(err)
	}

	st = stats(root)
	fmt.Println(st)
	printHamt(root)
	if st.totalNodes != 1 || st.totalKvs != 3 || st.counts[3] != 1 {
		t.Fatal("Should be 1 node with 1 bucket")
	}

	// should have the same CID as original
	c, err = cs.Put(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Equals(baseCid) {
		t.Fatal("CID mismatch on mutation")
	}
}

func addAndRemoveKeys(t *testing.T, keys []string, extraKeys []string, options ...Option) {
	ctx := context.Background()
	vals := make(map[string][]byte)
	for i := 0; i < len(keys); i++ {
		s := keys[i]
		vals[s] = randValue()
	}

	cs := cbor.NewCborStore(newMockBlocks())
	begn := NewNode(cs, options...)
	for _, k := range keys {
		if err := begn.Set(ctx, k, vals[k]); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("start flush")
	bef := time.Now()
	if err := begn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("flush took: ", time.Since(bef))
	c, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}

	var n Node
	if err := cs.Get(ctx, c, &n); err != nil {
		t.Fatal(err)
	}
	n.store = cs
	n.hash = begn.hash
	n.bitWidth = begn.bitWidth
	for k, v := range vals {
		var out []byte
		err := n.Find(ctx, k, &out)
		if err != nil {
			t.Fatalf("should have found the thing (err: %s)", err)
		}
		if !bytes.Equal(out, v) {
			t.Fatalf("got wrong value after value change: %x != %x", out, v)
		}
	}

	printHamt(begn)

	// create second hamt by adding and deleting the extra keys
	for i := 0; i < len(extraKeys); i++ {
		begn.Set(ctx, extraKeys[i], randValue())
	}
	for i := 0; i < len(extraKeys); i++ {
		if err := begn.Delete(ctx, extraKeys[i]); err != nil {
			t.Fatal(err)
		}
	}

	if err := begn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	c2, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}

	var n2 Node
	if err := cs.Get(ctx, c2, &n2); err != nil {
		t.Fatal(err)
	}
	n2.store = cs
	n2.hash = begn.hash
	n2.bitWidth = begn.bitWidth
	if !nodesEqual(t, cs, &n, &n2) {
		t.Fatal("nodes should be equal")
	}
}

func printHamt(hamt *Node) {
	ctx := context.Background()

	var printNode func(n *Node, depth int)

	printNode = func(n *Node, depth int) {
		c, err := n.store.Put(ctx, n)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s‣ %v:\n", strings.Repeat("  ", depth), c)
		for _, p := range n.Pointers {
			if p.isShard() {
				child, err := p.loadChild(ctx, n.store, n.bitWidth, n.hash)
				if err != nil {
					panic(err)
				}
				printNode(child, depth+1)
			} else {
				var keys []string
				for _, pt := range p.KVs {
					keys = append(keys, string(pt.Key))
				}
				fmt.Printf("%s⇶ [ %s ]\n", strings.Repeat("  ", depth+1), strings.Join(keys, ", "))
			}
		}
	}

	printNode(hamt, 0)
}

func dotGraphRec(n *Node, name *int) {
	cur := *name
	for _, p := range n.Pointers {
		if p.isShard() {
			*name++
			fmt.Printf("\tn%d -> n%d;\n", cur, *name)
			nd, err := p.loadChild(context.Background(), n.store, n.bitWidth, n.hash)
			if err != nil {
				panic(err)
			}

			dotGraphRec(nd, name)
		} else {
			var names []string
			for _, pt := range p.KVs {
				names = append(names, string(pt.Key))
			}
			fmt.Printf("\tn%d -> n%s;\n", cur, strings.Join(names, "_"))
		}
	}
}

type hamtStats struct {
	totalNodes int
	totalKvs   int
	counts     map[int]int
}

func (hs hamtStats) String() string {
	return fmt.Sprintf("nodes=%d, kvs=%d, counts=%v", hs.totalNodes, hs.totalKvs, hs.counts)
}

func stats(n *Node) *hamtStats {
	st := &hamtStats{counts: make(map[int]int)}
	statsrec(n, st)
	return st
}

func statsrec(n *Node, st *hamtStats) {
	st.totalNodes++
	for _, p := range n.Pointers {
		if p.isShard() {
			nd, err := p.loadChild(context.Background(), n.store, n.bitWidth, n.hash)
			if err != nil {
				panic(err)
			}

			statsrec(nd, st)
		} else {
			st.totalKvs += len(p.KVs)
			st.counts[len(p.KVs)]++
		}
	}
}

func TestHash(t *testing.T) {
	h1 := defaultHashFunction([]byte("abcd"))
	h2 := defaultHashFunction([]byte("abce"))
	if h1[0] == h2[0] && h1[1] == h2[1] && h1[3] == h2[3] {
		t.Fatal("Hash should give different strings different hash prefixes")
	}
}

func TestBasic(t *testing.T) {
	testBasic(t)
}

func TestSha256(t *testing.T) {
	testBasic(t, UseHashFunction(func(in []byte) []byte {
		out := sha256.Sum256(in)
		return out[:]
	}))
}

func testBasic(t *testing.T, options ...Option) {
	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())
	begn := NewNode(cs, options...)

	val := []byte("cat dog bear")
	if err := begn.Set(ctx, "foo", val); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		if err := begn.Set(ctx, randString(), randValue()); err != nil {
			t.Fatal(err)
		}
	}

	if err := begn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}

	n, err := LoadNode(ctx, cs, c, options...)
	if err != nil {
		t.Fatal(err)
	}

	var out []byte
	if err := n.Find(ctx, "foo", &out); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out, val) {
		t.Fatal("out bytes were wrong: ", out)
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())
	begn := NewNode(cs)

	val := []byte("cat dog bear")
	if err := begn.Set(ctx, "foo", val); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := begn.Set(ctx, randString(), randValue()); err != nil {
			t.Fatal(err)
		}
	}

	if err := begn.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}

	n, err := LoadNode(ctx, cs, c)
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Delete(ctx, "foo"); err != nil {
		t.Fatal(err)
	}

	var out []byte
	if err := n.Find(ctx, "foo", &out); err == nil {
		t.Fatal("shouldnt have found object")
	}
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

	cs := cbor.NewCborStore(newMockBlocks())
	begn := NewNode(cs)
	for _, k := range keys {
		if err := begn.Set(ctx, k, vals[k]); err != nil {
			t.Fatal(err)
		}
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
	fmt.Println("flush took: ", time.Since(bef))
	c, err := cs.Put(ctx, begn)
	if err != nil {
		t.Fatal(err)
	}

	var n Node
	if err := cs.Get(ctx, c, &n); err != nil {
		t.Fatal(err)
	}
	n.store = cs
	n.hash = defaultHashFunction
	n.bitWidth = defaultBitWidth
	bef = time.Now()
	//for k, v := range vals {
	for _, k := range keys {
		v := vals[k]
		var out []byte
		if err := n.Find(ctx, k, &out); err != nil {
			t.Fatal("should have found the thing: ", err)
		}
		if !bytes.Equal(out, v) {
			t.Fatal("got wrong value")
		}
	}
	fmt.Println("finds took: ", time.Since(bef))

	for i := 0; i < 100; i++ {
		err := n.Find(ctx, randString(), nil)
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
		var out []byte
		err := n.Find(ctx, k, &out)
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
		if err := n.Find(ctx, k, nil); err != ErrNotFound {
			t.Fatal("Expected ErrNotFound, got: ", err)
		}
	}
}

func nodesEqual(t *testing.T, store cbor.IpldStore, n1, n2 *Node) bool {
	ctx := context.Background()
	err := n1.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n1Cid, err := store.Put(ctx, n1)
	if err != nil {
		t.Fatal(err)
	}
	err = n2.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n2Cid, err := store.Put(ctx, n2)
	if err != nil {
		t.Fatal(err)
	}
	return n1Cid.Equals(n2Cid)
}

func TestReloadEmpty(t *testing.T) {
	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())

	n := NewNode(cs)
	c, err := cs.Put(ctx, n)
	if err != nil {
		t.Fatal(err)
	}

	on, err := LoadNode(ctx, cs, c)
	if err != nil {
		t.Fatal(err)
	}

	if err := on.Set(ctx, "foo", "bar"); err != nil {
		t.Fatal(err)
	}
}

func TestCopy(t *testing.T) {
	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())

	n := NewNode(cs)
	nc := n.Copy()
	if !nodesEqual(t, cs, n, nc) {
		t.Fatal("nodes should be equal")
	}
	n.Set(ctx, "key", []byte{0x01})
	if nodesEqual(t, cs, n, nc) {
		t.Fatal("nodes should not be equal -- we set a key on n")
	}
	nc = n.Copy()
	nc.Set(ctx, "key2", []byte{0x02})
	if nodesEqual(t, cs, n, nc) {
		t.Fatal("nodes should not be equal -- we set a key on nc")
	}
	n = nc.Copy()
	if !nodesEqual(t, cs, n, nc) {
		t.Fatal("nodes should be equal")
	}
}

func TestCopyCopiesNilSlices(t *testing.T) {
	cs := cbor.NewCborStore(newMockBlocks())

	n := NewNode(cs)
	pointer := &Pointer{}
	n.Pointers = append(n.Pointers, pointer)

	if n.Pointers[0].KVs != nil {
		t.Fatal("Expected uninitialize slice to be nil")
	}

	nc := n.Copy()

	if nc.Pointers[0].KVs != nil {
		t.Fatal("Expected copied nil slices to be nil")
	}
}

func TestCopyWithoutFlush(t *testing.T) {
	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())

	count := 200
	n := NewNode(cs)
	for i := 0; i < count; i++ {
		n.Set(ctx, fmt.Sprintf("key%d", i), []byte{byte(i)})
	}

	n.Flush(ctx)

	for i := 0; i < count; i++ {
		n.Set(ctx, fmt.Sprintf("key%d", i), []byte{byte(count + i)})
	}

	nc := n.Copy()

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key%d", i)

		var val []byte
		if err := n.Find(ctx, key, &val); err != nil {
			t.Fatalf("should have found key %s in original", key)
		}

		var valCopy []byte
		if err := nc.Find(ctx, key, &valCopy); err != nil {
			t.Fatalf("should have found key %s in copy", key)
		}

		if val[0] != valCopy[0] {
			t.Fatalf("copy does not equal original (%d != %d)", valCopy[0], val[0])
		}
	}
}

func TestValueLinking(t *testing.T) {
	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())

	thingy1 := map[string]string{"cat": "dog"}
	c1, err := cs.Put(ctx, thingy1)
	if err != nil {
		t.Fatal(err)
	}

	thingy2 := map[string]interface{}{
		"one": c1,
		"foo": "bar",
	}

	n := NewNode(cs)

	if err := n.Set(ctx, "cat", thingy2); err != nil {
		t.Fatal(err)
	}

	tcid, err := cs.Put(ctx, n)
	if err != nil {
		t.Fatal(err)
	}

	blk, err := cs.Blocks.Get(tcid)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("BLOCK DATA: %x\n", blk.RawData())
	nd, err := cbor.DecodeBlock(blk)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("thingy1", c1)
	fmt.Println(nd.Links()[0])
}

func TestSetNilValues(t *testing.T) {
	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())

	n := NewNode(cs)

	k := make([]byte, 1)

	for i := 0; i < 500; i++ {
		k[0] = byte(i)
		var um cbg.CBORMarshaler
		if err := n.Set(ctx, string(k), um); err != nil {
			t.Fatal(err)
		}
	}

	nn := NewNode(cs)

	rc, err := cs.Put(ctx, nn)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		tn, err := LoadNode(ctx, cs, rc)
		if err != nil {
			t.Fatal(err)
		}

		k[0] = byte(i)
		var n cbg.CBORMarshaler
		if err := tn.Set(ctx, string(k), n); err != nil {
			t.Fatal(err)
		}

		if err := tn.Flush(ctx); err != nil {
			t.Fatal(err)
		}

		rc, err = cs.Put(ctx, tn)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Some tests that use manually constructed (and very basic) CBOR forms of
// nodes to test whether the implementation will reject malformed encoded nodes
// on load.
func TestMalformedHamt(t *testing.T) {
	ctx := context.Background()
	blocks := newMockBlocks()
	cs := cbor.NewCborStore(blocks)
	bcid, err := cid.Decode("bafy2bzaceab7vkg5c3zti7ebqensb3onksjkc4wwktkiledkezgvnbvzs4cti")
	bccid, err := cid.Decode("bafy2bzaceab7vkg5c3zti7ebqensb3onksjkc4wwktkiledkezgvnbvzs4cqa")
	if err != nil {
		t.Fatal(err)
	}
	// just the bcid bytes, without the tag
	cidBytes, _ := hex.DecodeString("000171A0E4022003FAA8DD16F3347C81811B20EDCD5492A172D654D485906A264D5686B9970534")
	// just the bccid bytes, without the tag, prefixed with 0x00 for dag-cbor
	ccidBytes, _ := hex.DecodeString("000171a0e4022003faa8dd16f3347c81811b20edcd5492a172d654d485906a264d5686b9970500")
	// badCidBytes is cidBytes but with dag-pb, prefixed with 0x00 for dag-cbor
	badCidBytes, _ := hex.DecodeString("000170a0e4022003faa8dd16f3347c81811b20edcd5492a172d654d485906a264d5686b9970534")

	// util closures
	store := func(blob []byte) {
		blocks.data[bcid] = block.NewBlock(blob)
	}
	load := func() *Node {
		n, err := LoadNode(ctx, cs, bcid, UseTreeBitWidth(8), UseHashFunction(identityHash))
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	find := func(key []byte, expected []byte) *[]byte {
		vg, err := load().FindRaw(ctx, string(key))
		if err != nil {
			t.Fatal(err)
		}
		// should find a bytes(1) "\xff"
		if !bytes.Equal(vg, expected) {
			return &vg
		}
		return nil
	}

	type kv struct {
		key   byte
		value byte
	}
	bucketCbor := func(kvs ...kv) []byte {
		en := []byte{}
		for _, kv := range kvs {
			en = bcat(en, bcat(b(0x80+2), // array(2)
				bcat(b(0x40+1), b(kv.key)),    // bytes(1) "\x??"
				bcat(b(0x40+1), b(kv.value)))) // bytes(1) "\x??"
		}
		return bcat(b(0xa0+1), // map(1)
			bcat(b(0x60+1), b(0x31)), // string(1) "1"
			bcat(b(0x80+byte(len(kvs))), // array(?)
				en)) // bucket contents
	}

	// most minimal HAMT node with one k/v entry, sanity check we can load this
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bucketCbor(kv{0x00, 0xff})))) // 0x00=0xff
	// should find a bytes(1) "\xff"
	find(b(0x00), bcat(b(0x40+1), b(0xff)))
	// print the raw cbor: fmt.Printf("%v\n", hex.EncodeToString(blocks.data[bcid].RawData()))

	// 10 entry node, assumed bitwidth of >3
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+2), []byte{0x03, 0xff}), // bytes(1) "\x3ff" (bitmap with lower 10 bits set)
			bcat(b(0x80+10), // array(10)
				bucketCbor(kv{0x00, 0xf0}),   // 0x00=0xf0
				bucketCbor(kv{0x01, 0xf1}),   // 0x01=0xf1
				bucketCbor(kv{0x02, 0xf2}),   // 0x02=0xf2
				bucketCbor(kv{0x03, 0xf3}),   // 0x03=0xf3
				bucketCbor(kv{0x04, 0xf4}),   // 0x04=0xf4
				bucketCbor(kv{0x05, 0xf5}),   // 0x05=0xf5
				bucketCbor(kv{0x06, 0xf6}),   // 0x06=0xf6
				bucketCbor(kv{0x07, 0xf7}),   // 0x07=0xf7
				bucketCbor(kv{0x08, 0xf8}),   // 0x08=0xf8
				bucketCbor(kv{0x09, 0xf9})))) // 0x09=0xf9
	// sanity check
	for i := 0; i < 10; i++ {
		v := bcat(b(0x40+1), b(0xf0+byte(i)))
		if vg := find(b(0x00+byte(i)), v); vg != nil {
			t.Fatalf("expected a value of %v, got %v", hex.EncodeToString(v), hex.EncodeToString(*vg))
		}
	}

	// load as bitWidth=3, which can only handle a max of 8 elements
	n, err := LoadNode(ctx, cs, bcid, UseTreeBitWidth(3), UseHashFunction(identityHash))
	if err != ErrMalformedHamt || n != nil {
		t.Fatal("Should have returned ErrMalformedHamt for too-small bitWidth")
	}

	// test that the bitfield set count matches array size
	// this node says it has 3 elements in the bitfield, but there are 4 buckets
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x03)), // bytes(1) "\x03" (bitmap)
			bcat(b(0x80+1), // array(1)
				bucketCbor(kv{0x00, 0xff}),   // 0x00=0xff
				bucketCbor(kv{0x00, 0xff}),   // 0x00=0xff
				bucketCbor(kv{0x00, 0xff}),   // 0x00=0xff
				bucketCbor(kv{0x00, 0xff})))) // 0x00=0xff
	n, err = LoadNode(ctx, cs, bcid, UseTreeBitWidth(3), UseHashFunction(identityHash))
	if err != ErrMalformedHamt || n != nil {
		t.Fatal("Should have returned ErrMalformedHamt for mismatch bitfield count")
	}

	// test mixed link & bucket

	// this node contains 2 elements, the first is a plain entry with one bucket
	// and with a single key of 0x0100, the second element is a link to a child
	// node which happens to be the same CID as this node will be stored in.
	// However, this second entry has both a CID and a bucket in the same
	// element, which is not allowed. Without checks for exactly one of these
	// two things then then a lookup for key 0x0100 would navigate through this
	// node and back again as its own child to the first element.
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x03)), // bytes(1) "\x03" (bitmap)
			bcat(b(0x80+2), // array(2)
				bcat(b(0xa0+1), // map(1)
					bcat(b(0x60+1), b(0x31)), // string(1) "1"
					bcat(b(0x80+1), // array(1)
						bcat(b(0x80+2), // array(2)
							bcat(b(0x40+2), []byte{0x01, 0x00}), // bytes(2) "\x0100"
							bcat(b(0x40+1), b(0xff))))),         // bytes(1) "\xff"
				bcat(b(0xa0+2), // map(2)
					bcat(b(0x60+1), b(0x30)), // string(1) "0"
					bcat(b(0xd8), b(0x2a), // tag(42)
						b(0x58), b(0x27), // bytes(39)
						cidBytes), // cid
					bcat(b(0x60+1), b(0x31)), // string(1) "1"
					bcat(b(0x80+1), // array(1)
						bcat(b(0x80+2), // array(2)
							bcat(b(0x40+1), b(0x01)),      // bytes(1) "\x00"
							bcat(b(0x40+1), b(0xfe)))))))) // bytes(1) "\xff

	n, err = LoadNode(ctx, cs, bcid, UseTreeBitWidth(8), UseHashFunction(identityHash))
	if err == nil || n != nil || err.Error() != "Pointers should be a single element map" {
		// no ErrMalformedHamt here possible bcause of cbor-gen wrapping
		t.Fatal("Should have returned error for bad Pointer cbor")
	}

	// test pointers with links have are DAG-CBOR multicodec
	// sanity check minimal node pointing to a child node
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bcat(b(0xa0+1), // map(1)
					bcat(b(0x60+1), b(0x30)), // string(1) "0"
					bcat(b(0xd8), b(0x2a), // tag(42)
						b(0x58), b(0x27), // bytes(39)
						cidBytes))))) // cid
	load()

	// node pointing to a non-dag-cbor node
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bcat(b(0xa0+1), // map(1)
					bcat(b(0x60+1), b(0x30)), // string(1) "0"
					bcat(b(0xd8), b(0x2a), // tag(42)
						b(0x58), b(0x27), // bytes(39)
						badCidBytes))))) // cid
	n, err = LoadNode(ctx, cs, bcid, UseTreeBitWidth(8), UseHashFunction(identityHash))
	if err != ErrMalformedHamt || n != nil {
		t.Fatal("Should have returned ErrMalformedHamt for bad child link codec")
	}

	// bucket with zero elements
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bucketCbor()))) // empty bucket
	n, err = LoadNode(ctx, cs, bcid, UseTreeBitWidth(8), UseHashFunction(identityHash))
	if err != ErrMalformedHamt || n != nil {
		t.Fatal("Should have returned ErrMalformedHamt for zero element bucket")
	}

	// bucket with 4 elements
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bucketCbor(
					kv{0x00, 0xff},
					kv{0x01, 0xff},
					kv{0x02, 0xff},
					kv{0x03, 0xff})))) // bucket with 4 entires

	n, err = LoadNode(ctx, cs, bcid, UseTreeBitWidth(8), UseHashFunction(identityHash))
	if err != ErrMalformedHamt || n != nil {
		t.Fatal("Should have returned ErrMalformedHamt for four element bucket")
	}

	// test KV buckets are ordered by key (bytewise comparison)

	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bucketCbor(
					kv{0x01, 0xff},
					kv{0x00, 0xff})))) // bucket with 2, misordered entries

	n, err = LoadNode(ctx, cs, bcid, UseTreeBitWidth(8), UseHashFunction(identityHash))
	if err != ErrMalformedHamt || n != nil {
		t.Fatal("Should have returned ErrMalformedHamt for mis-ordered bucket")
	}

	// test duplicate keys
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bucketCbor(
					kv{0x00, 0x01},
					kv{0x01, 0xf0},
					kv{0x01, 0xff})))) // bucket with 3 element, 2 dupes with different values

	n, err = LoadNode(ctx, cs, bcid, UseTreeBitWidth(8), UseHashFunction(identityHash))
	if err != ErrMalformedHamt || n != nil {
		t.Fatal("Should have returned ErrMalformedHamt for mis-ordered bucket")
	}

	// _the_ empty HAMT, should be possible, but special-case format for a roor node
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x00)), // bytes(1) "\x00" (bitmap)
			bcat(b(0x80+0))))         // array(0)
	load()

	// make a child empty block and point to it in a root
	blocks.data[bccid] = block.NewBlock(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x00)), // bytes(1) "\x00" (bitmap)
			bcat(b(0x80+0))))         // array(0)
	// root block pointing to the child, child block can't be empty
	store(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bcat(b(0xa0+1), // map(1)
					bcat(b(0x60+1), b(0x30)), // string(1) "0"
					bcat(b(0xd8), b(0x2a), // tag(42)
						b(0x58), b(0x27), // bytes(39)
						ccidBytes))))) // cid

	vg, err := load().FindRaw(ctx, string([]byte{0x00, 0x01}))
	// without validation of the child block, this would return an ErrNotFound
	if err != ErrMalformedHamt || vg != nil {
		t.Fatal("Should have returned ErrMalformedHamt for its empty child node")
	}

	// validate child block not allowed to have bucketSize or less lone-elements
	blocks.data[bccid] = block.NewBlock(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+1), // array(1)
				bucketCbor(kv{0x00, 0x01}))))
	vg, err = load().FindRaw(ctx, string([]byte{0x00, 0x01}))
	// without validation of the child block, this would return an ErrNotFound
	if err != ErrMalformedHamt || vg != nil {
		t.Fatal("Should have returned ErrMalformedHamt for its too-small child node")
	}

	blocks.data[bccid] = block.NewBlock(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x01)), // bytes(1) "\x01" (bitmap)
			bcat(b(0x80+3), // array(1)
				bucketCbor(kv{0x00, 0x01}),
				bucketCbor(kv{0x01, 0x01}),
				bucketCbor(kv{0x02, 0x01}))))
	vg, err = load().FindRaw(ctx, string([]byte{0x00, 0x01}))
	// without validation of the child block, this would return an ErrNotFound
	if err != ErrMalformedHamt || vg != nil {
		t.Fatal("Should have returned ErrMalformedHamt for its too-small child node")
	}

	// same as the above case, too few direct entries, but this one has a link in
	// it to a child so we can't perform this check, so this should work
	blocks.data[bccid] = block.NewBlock(
		bcat(b(0x80+2), // array(2)
			bcat(b(0x40+1), b(0x03)), // bytes(1) "\x03" (bitmap)
			bcat(b(0x80+2), // array(2)
				bcat(b(0xa0+1), // map(1)
					bcat(b(0x60+1), b(0x31)), // string(1) "1"
					bcat(b(0x80+1), // array(1)
						bcat(b(0x80+2), // array(2)
							bcat(b(0x40+2), []byte{0x00, 0x01}), // bytes(2) "\x0001"
							bcat(b(0x40+1), b(0xff))))),         // bytes(1) "\xff"
				bcat(b(0xa0+1), // map(1)
					bcat(b(0x60+1), b(0x30)), // string(1) "0"
					bcat(b(0xd8), b(0x2a), // tag(42)
						b(0x58), b(0x27), // bytes(39)
						ccidBytes))))) // cid

	vg, err = load().FindRaw(ctx, string([]byte{0x00, 0x01}))
	// without validation of the child block, this would return an ErrNotFound
	if err != nil && bytes.Compare(vg, []byte{0x40 + 2, 0x00, 0x01}) != 0 {
		t.Fatal("Should have returned found entry")
	}
}

func TestCleanChildOrdering(t *testing.T) {
	// This test originates from a case hit while testing filecoin-project/specs-actors.
	// TODO a HAMT exercising this case can very likely be constructed with many fewer
	// operations. I'm duplicating the full original test for expedience
	//
	// The important part of this HAMT is that at some point child with index 20 looks like:
	// P0 -- []KV{KV{Key: 0x01a0, Value: v0}}
	// P1 -- []KV{KV{Key: 0x01a8, Value: v1}}
	// P2 -- []KV{KV{Key: 0x0181, Value: v2}}
	// P3 -- []KV{KV{Key: 0x006c, Value: v2}}
	//
	// We then delete 0x006c.  This forces this child node into a bucket.
	// before writing this test cleanChild did not explicitly sort KVs from
	// all pointers, so the new bucket looked like:
	// []KV{
	//   KV{Key: 0x01a0, Value: v1},
	//   KV{Key: 0x01a8, Value: v2},
	//   KV{Key: 0x0181, Value: v2},
	// }
	//
	// This violated the buckets-are-sorted-by-key condition

	// Construct HAMT
	makeKey := func(i uint64) string {
		buf := make([]byte, 10)
		n := binary.PutUvarint(buf, i)
		return string(buf[:n])
	}
	dummyValue := []byte{0xaa, 0xbb, 0xcc, 0xdd}

	ctx := context.Background()
	cs := cbor.NewCborStore(newMockBlocks())
	hamtOptions := []Option{
		UseTreeBitWidth(5),
		UseHashFunction(func(input []byte) []byte {
			res := sha256.Sum256(input)
			return res[:]
		}),
	}

	h := NewNode(cs, hamtOptions...)

	for i := uint64(100); i < uint64(195); i++ {
		err := h.Set(ctx, makeKey(i), dummyValue)
		require.NoError(t, err)
	}

	// Shouldn't matter but repeating original case exactly
	require.NoError(t, h.Flush(ctx))
	root, err := cs.Put(ctx, h)
	require.NoError(t, err)
	h, err = LoadNode(ctx, cs, root, hamtOptions...)
	require.NoError(t, err)

	// Delete key 104 so child indexed at 20 has four pointers
	err = h.Delete(ctx, makeKey(104))
	assert.NoError(t, err)
	err = h.Delete(ctx, makeKey(108))
	assert.NoError(t, err)
	err = h.Flush(ctx)
	assert.NoError(t, err)
	root, err = cs.Put(ctx, h)

	// Reload without error
	require.NoError(t, err)
	h, err = LoadNode(ctx, cs, root, hamtOptions...)
	assert.NoError(t, err)
}
