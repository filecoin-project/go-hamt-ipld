package hamt

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sort"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
)

//-----------------------------------------------------------------------------
// Boolean constants
type overwrite bool

const (
	// use OVERWRITE for modifyValue operations that overwrite existing values
	OVERWRITE = overwrite(true)
	// use NOVERWRITE for modifyValue operations that cannot overwrite existing values
	NOVERWRITE = overwrite(false)
)

type modified bool

const (
	// return MODIFIED when a key value mapping is overwritten
	MODIFIED = modified(true)
	// return UNMODIFIED when a no key value mappings are overwritten
	UNMODIFIED = modified(false)
)

//-----------------------------------------------------------------------------
// Errors

// ErrMaxDepth is returned when the HAMT spans further than the hash function
// is capable of representing. This can occur when sufficient hash collisions
// (e.g. from a weak hash function and attacker-provided keys) extend leaf
// nodes beyond the number of bits that a hash can represent. Or this can occur
// on extremely large (likely impractical) HAMTs that are unable to be
// represented with the hash function used. Hash functions with larger byte
// output increase the maximum theoretical depth of a HAMT.
var ErrMaxDepth = fmt.Errorf("attempted to traverse HAMT beyond max-depth")

// ErrMalformedHamt is returned whenever a block intended as a HAMT node does
// not conform to the expected form that a block may take. This can occur
// during block-load where initial validation takes place or during traversal
// where certain conditions are expected to be met.
var ErrMalformedHamt = fmt.Errorf("HAMT node was malformed")

//-----------------------------------------------------------------------------
// Serialized data structures

// HashFunc is a hashing function for values.
type HashFunc func([]byte) []byte

// Node is a single point in the HAMT, encoded as an IPLD tuple in DAG-CBOR of
// shape:
//   [bytes, [Pointer...]]
// where 'bytes' is the big.Int#Bytes() and the Pointers array is between 1 and
// `2^bitWidth`.
//
// The Bitfield provides us with a mechanism to store a compacted array of
// Pointers. Each bit in the Bitfield represents an element in a sparse array
// where `1` indicates the element is present in the Pointers array and `0`
// indicates it is omitted. To look-up a specific index in the Pointers array
// you must first make a count of the number of `1`s (popcount) up to the
// element you are looking for.
// e.g. a Bitfield of `10010110000` shows that we have a 4 element Pointers
// array. Indexes `[1]` and `[2]` are not present, but index `[3]` is at
// the second position of our Pointers array.
//
// The IPLD Schema representation of this data structure is as follows:
//
// 		type Node struct {
// 			bitfield Bytes
// 			pointers [Pointer]
// 		} representation tuple
type Node struct {
	Bitfield *big.Int
	Pointers []*Pointer

	bitWidth int
	hash     HashFunc

	// for fetching and storing children
	store cbor.IpldStore
}

// Pointer is an element in a HAMT node's Pointers array, encoded as an IPLD
// tuple in DAG-CBOR of shape:
//   CID or [KV...]
// i.e. it is represented as a "kinded union" where a Link is a pointer to a
// child node, while an array is a bucket of elements local to this node. A
// Pointer must represent exactly one of of these two states and cannot be both
// (or neither).
//
// There are between 1 and 2^bitWidth of these Pointers in any HAMT node.
//
// A Pointer contains either a KV bucket of up to `bucketSize` (3) values or a
// link (CID) to a child node. When a KV bucket overflows beyond `bucketSize`,
// the bucket is replaced with a link to a newly created HAMT node which will
// contain the `bucketSize+1` elements in its own Pointers array.
//
// The IPLD Schema representation of this data structure is as follows:
//
// 		type Pointer union {
//			&Node link
// 			Bucket list
// 		} representation kinded
//
//		type Bucket [KV]
type Pointer struct {
	KVs  []*KV
	Link cid.Cid

	// cache is a pointer to an in-memory Node, which may or may not be
	// present, and corresponds to the Link field, which also may or may not
	// be present.
	//
	// If present, the cached Node should be semantically substitutable with
	// the Link field. It makes no sense for a cache Node to be present if KVs
	// is set. Link might not be set, if cache is present and is describing
	// data that has never yet been serialized and stored.
	//
	// `loadChild` will short circut to return this node if the pointer isn't
	// nil;
	// `loadChild` will also set this pointer when loading a node that wasn't
	// yet present cached.
	// `Flush` on a `Node` will iterate through each `Pointer` and `Put` its
	// cache node if:
	// 1. The Pointer's cache is not nil
	// 2. The Pointer's dirty flag is true
	// (and also recurse to `Flush` on that `Node`) -- in other words,
	// `Flush` writes out the cached data
	// `Flush` will assign `Link` in the process of `Put`'ing the 'cache' data.
	// `Copy` will copy any cached nodes, Link fields and dirty flags.
	//
	// `Link` becomes defined on`Flush`
	cache *Node
	// dirty flag to indicate that the cached node needs to be flushed
	dirty bool
}

// KV represents leaf storage within a HAMT node. A Pointer may hold up to
// `bucketSize` KV elements, where each KV contains a key and value pair
// stored by the user.
//
// Keys are represented as bytes.
//
// The IPLD Schema representation of this data structure is as follows:
//
//		type KV struct {
//			key Bytes
//			value Any
//		} representation tuple
type KV struct {
	Key   []byte
	Value *cbg.Deferred
}

//-----------------------------------------------------------------------------
// Instance and helpers functions

// NewNode creates a new IPLD HAMT Node with the given IPLD store and any
// additional options (bitWidth and hash function).
//
// This function creates a new HAMT that you can use directly and is also
// used internally to create child nodes.
func NewNode(cs cbor.IpldStore, options ...Option) (*Node, error) {
	cfg := defaultConfig()
	for _, option := range options {
		if err := option(cfg); err != nil {
			return nil, err
		}
	}

	return newNode(cs, cfg.hashFn, cfg.bitWidth), nil
}

// Find navigates through the HAMT structure to where key `k` should exist. If
// the key is not found, returns false. If the key is found, returns true, and
// if the `out` parameter has an UnmarshalCBOR(Reader) method, the
// value is decoded into it. The `out` parameter may be nil to test for existence
// without decoding.
//
// Depending on the size of the HAMT, this method may load a large number of
// child nodes via the HAMT's IpldStore.
func (n *Node) Find(ctx context.Context, k string, out cbg.CBORUnmarshaler) (bool, error) {
	var found bool
	err := n.getValue(ctx, &hashBits{b: n.hash([]byte(k))}, k, func(kv *KV) error {
		found = true
		// Note that an interface pointer-to-nil is not == nil and, if received here, will panic.
		if out == nil {
			return nil
		}
		return out.UnmarshalCBOR(bytes.NewReader(kv.Value.Raw))
	})
	return found, err
}

// FindRaw performs the same function as Find, but returns the raw bytes found
// at the key's location (which may or may not be DAG-CBOR, see also SetRaw).
func (n *Node) FindRaw(ctx context.Context, k string) (bool, []byte, error) {
	var found bool
	var value []byte
	err := n.getValue(ctx, &hashBits{b: n.hash([]byte(k))}, k, func(kv *KV) error {
		found = true
		value = kv.Value.Raw
		return nil
	})
	return found, value, err
}

// Delete removes an entry from the HAMT structure.
//
// Returns true if the key was found and deleted, false if the key was absent.
//
// This operation will result in the modification of _at least_ one IPLD block
// via the IpldStore. Depending on the contents of the leaf node, this
// operation may result in a node collapse to shrink the HAMT into its
// canonical form for the remaining data. For an insufficiently random
// collection of keys at the relevant leaf nodes such a collapse may cascade to
// further nodes.
func (n *Node) Delete(ctx context.Context, k string) (bool, error) {
	kb := []byte(k)
	modified, err := n.modifyValue(ctx, &hashBits{b: n.hash(kb)}, kb, nil, OVERWRITE)
	return modified == MODIFIED, err
}

// Constructs a new node value.
func newNode(cs cbor.IpldStore, hashFn HashFunc, bitWidth int) *Node {
	nd := &Node{
		Bitfield: big.NewInt(0),
		Pointers: make([]*Pointer, 0),
		bitWidth: bitWidth,
		hash:     hashFn,
		store:    cs,
	}
	return nd
}

// handle the two Find operations in a recursive manner, where each node in the
// HAMT we traverse we call this function again with the same parameters.
// Invokes the callback if and only if the key is found.
// Note that `hv` contains state and `hv.Next()` is not idempotent. Each call
// increments a counter for the number of bits consumed.
func (n *Node) getValue(ctx context.Context, hv *hashBits, k string, cb func(*KV) error) error {
	// hv.Next chomps off `bitWidth` bits from the hash digest. As we proceed
	// down the tree, each node takes `bitWidth` more bits from the digest. If
	// we attempt to take more bits than the digest contains, we hit max-depth
	// and can't proceed.
	idx, err := hv.Next(n.bitWidth)
	if err != nil {
		return ErrMaxDepth
	}

	// if the element expected at this node isn't here then we can be sure it
	// doesn't exist in the HAMT.
	if n.Bitfield.Bit(idx) == 0 {
		return nil
	}

	// otherwise, the value is either local or in a child

	// perform a popcount of bits up to the `idx` to find `cindex`
	cindex := byte(n.indexForBitPos(idx))

	c := n.getPointer(cindex)
	if c.isShard() {
		// if isShard, we have a pointer to a child that we need to load and
		// delegate our find operation to
		chnd, err := c.loadChild(ctx, n.store, n.bitWidth, n.hash)
		if err != nil {
			return err
		}

		return chnd.getValue(ctx, hv, k, cb)
	}

	// if not isShard, then the key/value pair is local and we need to retrieve
	// it from the bucket. The bucket is sorted but only between 1 and
	// `bucketSize` in length, so no need for fanciness.
	for _, kv := range c.KVs {
		if string(kv.Key) == k {
			return cb(kv)
		}
	}

	return nil
}

// load a HAMT node from the IpldStore and pass on the (assumed) parameters
// that are not stored with the node.
func (p *Pointer) loadChild(ctx context.Context, ns cbor.IpldStore, bitWidth int, hash HashFunc) (*Node, error) {
	if p.cache != nil {
		return p.cache, nil
	}

	out, err := loadNode(ctx, ns, p.Link, false, bitWidth, hash)
	if err != nil {
		return nil, err
	}

	p.cache = out
	return out, nil
}

// LoadNode loads a HAMT Node from the IpldStore and configures it according
// to any specified Option parameters. Where the parameters of this HAMT vary
// from the defaults (hash function and bitWidth), those variations _must_ be
// supplied here via Options otherwise the HAMT will not be readable.
//
// Users should consider how their HAMT parameters are stored or specified
// along with their HAMT where the data is expected to have a long shelf-life
// as future users will need to know the parameters of a HAMT being loaded in
// order to decode it. Users should also NOT rely on the default parameters
// of this library to remain the defaults long-term and have strategies in
// place to manage variations.
func LoadNode(ctx context.Context, cs cbor.IpldStore, c cid.Cid, options ...Option) (*Node, error) {
	cfg := defaultConfig()
	for _, option := range options {
		if err := option(cfg); err != nil {
			return nil, err
		}
	}
	return loadNode(ctx, cs, c, true, cfg.bitWidth, cfg.hashFn)
}

// internal version of loadNode that is aware of whether this is a root node or
// not for the purpose of additional validation on non-root nodes.
func loadNode(
	ctx context.Context,
	cs cbor.IpldStore,
	c cid.Cid,
	isRoot bool,
	bitWidth int,
	hashFunction HashFunc,
) (*Node, error) {
	var out Node
	if err := cs.Get(ctx, c, &out); err != nil {
		return nil, err
	}

	out.store = cs
	out.bitWidth = bitWidth
	out.hash = hashFunction

	// Validation

	// too many elements in the data array for the configured bitWidth?
	if len(out.Pointers) > 1<<uint(out.bitWidth) {
		return nil, ErrMalformedHamt
	}

	// the bifield is lying or the elements array is
	if out.bitsSetCount() != len(out.Pointers) {
		return nil, ErrMalformedHamt
	}

	for _, ch := range out.Pointers {
		isLink := ch.isShard()
		isBucket := ch.KVs != nil
		if !((isLink && !isBucket) || (!isLink && isBucket)) {
			// Pointer#UnmarshalCBOR shouldn't allow this
			return nil, ErrMalformedHamt
		}
		if isLink && ch.Link.Type() != cid.DagCBOR { // not dag-cbor
			return nil, ErrMalformedHamt
		}
		if isBucket {
			if len(ch.KVs) == 0 || len(ch.KVs) > bucketSize {
				return nil, ErrMalformedHamt
			}
			for i := 1; i < len(ch.KVs); i++ {
				if bytes.Compare(ch.KVs[i-1].Key, ch.KVs[i].Key) >= 0 {
					return nil, ErrMalformedHamt
				}
			}
		}
	}

	if !isRoot {
		// the only valid empty node is a root node
		if len(out.Pointers) == 0 {
			return nil, ErrMalformedHamt
		}
		// a non-root node that contains <=bucketSize direct elements should not
		// exist under compaction rules
		if out.directChildCount() == 0 && out.directKVCount() <= bucketSize {
			return nil, ErrMalformedHamt
		}
	}

	return &out, nil
}

// checkSize computes the total serialized size of the entire HAMT.
// It both puts and loads blocks as necesary to do this
// (using the Put operation and a paired Get to discover the serial size,
// and the load to move recursively as necessary).
//
// This is an expensive operation and should only be used in testing and analysis.
//
// Note that checkSize *does* actually *use the blockstore*: therefore it
// will affect get and put counts (and makes no attempt to avoid duplicate puts!);
// be aware of this if you are measuring those event counts.
func (n *Node) checkSize(ctx context.Context) (uint64, error) {
	c, err := n.store.Put(ctx, n)
	if err != nil {
		return 0, err
	}

	var def cbg.Deferred
	if err := n.store.Get(ctx, c, &def); err != nil {
		return 0, nil
	}

	totsize := uint64(len(def.Raw))
	for _, ch := range n.Pointers {
		if ch.isShard() {
			chnd, err := ch.loadChild(ctx, n.store, n.bitWidth, n.hash)
			if err != nil {
				return 0, err
			}
			chsize, err := chnd.checkSize(ctx)
			if err != nil {
				return 0, err
			}
			totsize += chsize
		}
	}

	return totsize, nil
}

// Flush has two effectis, it (partially!) persists data and resets dirty flag
//
// Flush operates recursively, telling each "cache" child node to flush;
// Put'ing that "cache" node to the store;
// updating this node's Link to the CID resulting from the store Put;
// clearing the dirty flag of that pointer to flase
// and then returning.
// Flush doesn't operate unless there's a "cache" node.
//
// "cache" nodes were previously storing either updated values,
// or, simply storing previously loaded data; these are disambiguated by the
// dirty flag which is true when the cache node's data has not been persisted
//
// Notice that Flush _does not_ Put _this node_.
// To fully persist changes, the caller still needs to Put this node to the
// store themselves, and store the new resulting Link wherever they expect the
// updated HAMT to be seen.
func (n *Node) Flush(ctx context.Context) error {
	for _, p := range n.Pointers {
		if p.cache != nil && p.dirty {
			if err := p.cache.Flush(ctx); err != nil {
				return err
			}

			c, err := n.store.Put(ctx, p.cache)
			if err != nil {
				return err
			}

			p.dirty = false
			p.Link = c
		}
	}
	return nil
}

// Set key k to value v, where v is has a MarshalCBOR(bytes.Buffer) method to
// encode it.
//
// To fully commit the change, it is necessary to Flush the root Node,
// and then additionally Put the root node to the store itself,
// and save the resulting CID wherever you expect the HAMT root to persist.
func (n *Node) Set(ctx context.Context, k string, v cbg.CBORMarshaler) error {
	var d cbg.Deferred
	if v == nil {
		d.Raw = cbg.CborNull
	} else {
		valueBuf := new(bytes.Buffer)
		if err := v.MarshalCBOR(valueBuf); err != nil {
			return err
		}
		d.Raw = valueBuf.Bytes()
	}

	keyBytes := []byte(k)
	_, err := n.modifyValue(ctx, &hashBits{b: n.hash(keyBytes)}, keyBytes, &d, OVERWRITE)
	return err
}

// SetIfAbsent sets key k to value v only if k is not already set to some value.
// Returns true if the value mapped to k is changed by this operation
// false otherwise.
func (n *Node) SetIfAbsent(ctx context.Context, k string, v cbg.CBORMarshaler) (bool, error) {
	var d cbg.Deferred
	if v == nil {
		d.Raw = cbg.CborNull
	} else {
		valueBuf := new(bytes.Buffer)
		if err := v.MarshalCBOR(valueBuf); err != nil {
			return false, err
		}
		d.Raw = valueBuf.Bytes()
	}

	keyBytes := []byte(k)
	modified, err := n.modifyValue(ctx, &hashBits{b: n.hash(keyBytes)}, keyBytes, &d, NOVERWRITE)
	return bool(modified), err
}

// SetRaw is similar to Set but sets key k in the HAMT to raw bytes without
// performing a DAG-CBOR marshal. The bytes may or may not be encoded DAG-CBOR
// (see also FindRaw for fetching raw form).
func (n *Node) SetRaw(ctx context.Context, k string, raw []byte) error {
	d := &cbg.Deferred{Raw: raw}
	kb := []byte(k)
	_, err := n.modifyValue(ctx, &hashBits{b: n.hash(kb)}, kb, d, OVERWRITE)
	return err
}

// the number of links to child nodes this node contains
func (n *Node) directChildCount() int {
	count := 0
	for _, p := range n.Pointers {
		if p.isShard() {
			count++
		}
	}
	return count
}

// the number of KV entries this node contains
func (n *Node) directKVCount() int {
	count := 0
	for _, p := range n.Pointers {
		if !p.isShard() {
			count = count + len(p.KVs)
		}
	}
	return count
}

// This happens after deletes to ensure that we retain canonical form for the
// given set of data this HAMT contains. This is a key part of the CHAMP
// algorithm. Any node that could be represented as a bucket in a parent node
// should be collapsed as such. This collapsing process could continue back up
// the tree as far as necessary to represent the data in the minimal HAMT form.
// This operation is done from a parent perspective, so we clean the child
// below us first and then our parent cleans us.
func (n *Node) cleanChild(chnd *Node, cindex byte) error {
	if chnd.directChildCount() != 0 {
		// child has its own children, nothing to collapse
		return nil
	}

	if chnd.directKVCount() > bucketSize {
		// child contains more local elements than could be collapsed
		return nil
	}

	if len(chnd.Pointers) == 1 {
		// The case where the child node has a single bucket, which we know can
		// only contain `bucketSize` elements (maximum), so we need to pull that
		// bucket up into this node.
		// This case should only happen when it bubbles up from the case below
		// where a lower child has its elements compacted into a single bucket. We
		// shouldn't be able to reach this block unless a delete has been
		// performed on a lower block and we are performing a post-delete clean on
		// a parent block.
		return n.setPointer(cindex, chnd.Pointers[0])
	}

	// The case where the child node contains enough elements to fit in a
	// single bucket and therefore can't justify its existence as a node on its
	// own. So we collapse all entries into a single bucket and replace the
	// link to the child with that bucket.
	// This may cause cascading collapses if this is the only bucket in the
	// current node, that case will be handled by our parent node by the l==1
	// case above.
	var chvals []*KV
	for _, p := range chnd.Pointers {
		chvals = append(chvals, p.KVs...)
	}
	kvLess := func(i, j int) bool {
		ki := chvals[i].Key
		kj := chvals[j].Key
		return bytes.Compare(ki, kj) < 0
	}
	sort.Slice(chvals, kvLess)

	return n.setPointer(cindex, &Pointer{KVs: chvals})
}

// Add a new value, update an existing value, or delete a value from the HAMT,
// potentially recursively calling child nodes to find the exact location of
// the entry in question and potentially collapsing nodes into buckets in
// parent nodes where a deletion violates the canonical form rules (see
// cleanNode()). Recursive calls use the same arguments on child nodes but
// note that `hv.Next()` is not idempotent. Each call will increment the number
// of bits chomped off the hash digest for this key.
func (n *Node) modifyValue(ctx context.Context, hv *hashBits, k []byte, v *cbg.Deferred, replace overwrite) (modified, error) {
	idx, err := hv.Next(n.bitWidth)
	if err != nil {
		return UNMODIFIED, ErrMaxDepth
	}

	// if the element expected at this node isn't here then we can be sure it
	// doesn't exist in the HAMT already and can insert it at the appropriate
	// position.
	if n.Bitfield.Bit(idx) != 1 {
		if v == nil { // Delete absent key
			return UNMODIFIED, nil
		}
		return MODIFIED, n.insertKV(idx, k, v)
	}

	// otherwise, the value is either local or in a child

	// perform a popcount of bits up to the `idx` to find `cindex`
	cindex := byte(n.indexForBitPos(idx))

	child := n.getPointer(cindex)
	if child.isShard() {
		// if isShard, we have a pointer to a child that we need to load and
		// delegate our modify operation to.
		// Note that this loadChild operation will cause the loaded node to be
		// "cached" and this pointer to be marked as dirty;
		// it is an eventual Flush passing back over this "cache" node which
		// causes the updates made to the in-memory "cache" node to eventually
		// be persisted.
		chnd, err := child.loadChild(ctx, n.store, n.bitWidth, n.hash)
		if err != nil {
			return UNMODIFIED, err
		}

		modified, err := chnd.modifyValue(ctx, hv, k, v, replace)
		if err != nil {
			return UNMODIFIED, err
		}

		if modified {
			// if we are modifying set the child.dirty
			// if we are not modifying leave it be, another operation might had set it previously
			child.dirty = true
		}

		// CHAMP optimization, ensure the HAMT retains its canonical form for the
		// current data it contains. This may involve collapsing child nodes if
		// they no longer contain enough elements to justify their stand-alone
		// existence.
		if v == nil {
			if err := n.cleanChild(chnd, cindex); err != nil {
				return UNMODIFIED, err
			}
		}

		return modified, nil
	}

	// if not isShard, then either the key/value pair is local here and can be
	// modified (or deleted) here or needs to be added as a new child node if
	// there is an overflow.

	if v == nil {
		// delete operation, find the child and remove it, compacting the bucket in
		// the process
		for i, p := range child.KVs {
			if bytes.Equal(p.Key, k) {
				if len(child.KVs) == 1 {
					// last element in the bucket, remove it and update the bitfield
					return MODIFIED, n.rmPointer(cindex, idx)
				}

				copy(child.KVs[i:], child.KVs[i+1:])
				child.KVs = child.KVs[:len(child.KVs)-1]
				return MODIFIED, nil
			}
		}
		return UNMODIFIED, nil // Delete absent key
	}

	// modify existing, check if key already exists
	for _, p := range child.KVs {
		if bytes.Equal(p.Key, k) {
			if bool(replace) && !bytes.Equal(p.Value.Raw, v.Raw) {
				p.Value = v
				return MODIFIED, nil
			}
			return UNMODIFIED, nil
		}
	}

	if len(child.KVs) >= bucketSize {
		// bucket is full, create a child node (shard) with all existing bucket
		// elements plus the new one and set it in the place of the bucket
		sub := newNode(n.store, n.hash, n.bitWidth)
		hvcopy := &hashBits{b: hv.b, consumed: hv.consumed}
		if _, err := sub.modifyValue(ctx, hvcopy, k, v, replace); err != nil {
			return UNMODIFIED, err
		}

		for _, p := range child.KVs {
			chhv := &hashBits{b: n.hash(p.Key), consumed: hv.consumed}
			if _, err := sub.modifyValue(ctx, chhv, p.Key, p.Value, replace); err != nil {
				return UNMODIFIED, err
			}
		}

		return MODIFIED, n.setPointer(cindex, &Pointer{cache: sub, dirty: true})
	}

	// otherwise insert the new element into the array in order, the ordering is
	// important to retain canonical form
	np := &KV{Key: k, Value: v}
	for i := 0; i < len(child.KVs); i++ {
		if bytes.Compare(k, child.KVs[i].Key) < 0 {
			child.KVs = append(child.KVs[:i], append([]*KV{np}, child.KVs[i:]...)...)
			return MODIFIED, nil
		}
	}
	child.KVs = append(child.KVs, np)
	return MODIFIED, nil
}

// Insert a new key/value pair into the current node at the specified index.
// This will involve modifying the bitfield for that index and inserting a new
// bucket containing the single key/value pair at that position.
func (n *Node) insertKV(idx int, k []byte, v *cbg.Deferred) error {
	i := n.indexForBitPos(idx)
	n.Bitfield.SetBit(n.Bitfield, idx, 1)

	p := &Pointer{KVs: []*KV{{Key: k, Value: v}}}

	n.Pointers = append(n.Pointers[:i], append([]*Pointer{p}, n.Pointers[i:]...)...)
	return nil
}

// Set a Pointer at a specific location, this doesn't modify the elements array
// but assumes that what's there can be updated. This seems to mostly be useful
// for tail calls.
func (n *Node) setPointer(i byte, p *Pointer) error {
	n.Pointers[i] = p
	return nil
}

// Remove a child at a specified index, splicing the Pointers array to remove
// it and updating the bitfield to specify that an element no longer exists at
// that position.
func (n *Node) rmPointer(i byte, idx int) error {
	copy(n.Pointers[i:], n.Pointers[i+1:])
	n.Pointers = n.Pointers[:len(n.Pointers)-1]
	n.Bitfield.SetBit(n.Bitfield, idx, 0)

	return nil
}

// Load a Pointer from the specified index of the Pointers array. The element
// should exist in a properly formed HAMT.
func (n *Node) getPointer(i byte) *Pointer {
	if int(i) >= len(n.Pointers) || i < 0 {
		// TODO(rvagg): I think this should be an error, there's an assumption in
		// calling code that it's not null and a proper hash chomp shouldn't result
		// in anything out of bounds
		return nil
	}

	return n.Pointers[i]
}

// Copy a HAMT node and all of its contents. May be useful for mutation
// operations where the original needs to be preserved in memory.
//
// This operation will also recursively clone any child nodes that are attached
// as cached nodes.
func (n *Node) Copy() *Node {
	// TODO(rvagg): clarify what situations this method is actually useful for.
	nn := newNode(n.store, n.hash, n.bitWidth)
	nn.Bitfield.Set(n.Bitfield)
	nn.Pointers = make([]*Pointer, len(n.Pointers))

	for i, p := range n.Pointers {
		pp := &Pointer{}
		if p.cache != nil {
			pp.cache = p.cache.Copy()
			pp.dirty = p.dirty
		}
		pp.Link = p.Link
		if p.KVs != nil {
			pp.KVs = make([]*KV, len(p.KVs))
			for j, kv := range p.KVs {
				pp.KVs[j] = &KV{Key: kv.Key, Value: kv.Value}
			}
		}
		nn.Pointers[i] = pp
	}

	return nn
}

// Pointers elements can either contain a bucket of local elements or be a
// link to a child node. In the case of a link, isShard() returns true.
func (p *Pointer) isShard() bool {
	return p.cache != nil || p.Link.Defined()
}

// ForEach recursively calls function f on each k / val pair found in the HAMT.
// This performs a full traversal of the graph and for large HAMTs can cause
// a large number of loads from the underlying store.
// The values are returned as raw bytes, not decoded.
func (n *Node) ForEach(ctx context.Context, f func(k string, val *cbg.Deferred) error) error {
	for _, p := range n.Pointers {
		if p.isShard() {
			chnd, err := p.loadChild(ctx, n.store, n.bitWidth, n.hash)
			if err != nil {
				return err
			}

			if err := chnd.ForEach(ctx, f); err != nil {
				return err
			}
		} else {
			for _, kv := range p.KVs {
				if err := f(string(kv.Key), kv.Value); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
