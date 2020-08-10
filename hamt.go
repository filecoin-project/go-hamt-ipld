package hamt

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

//-----------------------------------------------------------------------------
// Defaults

const bucketSize = 3
const defaultBitWidth = 8

//-----------------------------------------------------------------------------
// Errors

// ErrNotFound is returned when a Find operation fails to locate the specified
// key in the HAMT
var ErrNotFound = fmt.Errorf("not found")

// ErrMaxDepth is returned when the HAMT spans further than the hash function
// is capable of representing. This can occur when sufficient hash collisions
// (e.g. from a weak hash function and attacker-provided keys) extend leaf
// nodes beyond the number of bits that a hash can represent. Or this can occur
// on extremely large (likely impractical) HAMTs that are unable to be
// represented with the hash function used. Hash functions with larger byte
// output increase the maximum theoretical depth of a HAMT.
var ErrMaxDepth = fmt.Errorf("attempted to traverse hamt beyond max depth")

//-----------------------------------------------------------------------------
// Serialized data structures

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
// (Note: the `refmt` tags are ignored by cbor-gen which will generate an
// array type rather than map.)
//
// The IPLD Schema representation of this data structure is as follows:
//
// 		type Node struct {
// 			bitfield Bytes
// 			pointers [Pointer]
// 		} representation tuple
type Node struct {
	Bitfield *big.Int   `refmt:"bf"`
	Pointers []*Pointer `refmt:"p"`

	bitWidth int
	hash     func([]byte) []byte

	// for fetching and storing children
	store cbor.IpldStore
}

// Pointer is an element in a HAMT node's Pointers array, encoded as an IPLD
// tuple in DAG-CBOR of shape:
//   {"0": CID} or {"1": [KV...]}
// Where a map with a single key of "0" contains a Link, where a map with a
// single key of "1" contains a KV bucket. The map may contain only one of
// these two possible keys.
//
// There are between 1 and 2^bitWidth of these Pointers in any HAMT node.
//
// A Pointer contains either a KV bucket of up to `bucketSize` (3) values or a
// link (CID) to a child node. When a KV bucket overflows beyond `bucketSize`,
// the bucket is replaced with a link to a newly created HAMT node which will
// contain the `bucketSize+1` elements in its own Pointers array.
//
// (Note: the `refmt` tags are ignored by cbor-gen which will generate an
// array type rather than map.)
//
// The IPLD Schema representation of this data structure is as follows:
//
// 		type Pointer union {
//			&Node "0"
// 			Bucket "1"
// 		} representation keyed
//
//		type Bucket [KV]
type Pointer struct {
	KVs  []*KV   `refmt:"v,omitempty"`
	Link cid.Cid `refmt:"l,omitempty"`

	// cached node to avoid too many serialization operations
	// TODO(rvagg): we should check that this is actually used optimally. Flush()
	// performs a save of all of the cached nodes, but both Copy() and loadChild()
	// will set them. In the case of loadChild() we're not expecting a mutation so
	// a save is likely going to mean we incur unnecessary serialization when
	// we've simply inspected the tree. Copy() will only set a cached form if
	// it already exists on the source. It's unclear exactly what Flush() is good
	// for in its current form. Users may also need an advisory about memory
	// usage of large graphs since they don't have control over this outside of
	// Flush().
	cache *Node
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
// Options

// Option is a function that configures the node
//
// See UseTreeBitWidth and UseHashFunction
type Option func(*Node)

// UseTreeBitWidth allows you to set a custom bitWidth of the HAMT in bits
// (from 1-8).
//
// Passing in the returned Option to NewNode will generate a new HAMT that uses
// the specified bitWidth.
//
// The default bitWidth is 8.
func UseTreeBitWidth(bitWidth int) Option {
	return func(nd *Node) {
		if bitWidth > 0 && bitWidth <= 8 {
			nd.bitWidth = bitWidth
		}
	}
}

// UseHashFunction allows you to set the hash function used for internal
// indexing by the HAMT.
//
// Passing in the returned Option to NewNode will generate a new HAMT that uses
// the specified hash function.
//
// The default hash function is murmur3-x64 but you should use a
// cryptographically secure function such as SHA2-256 if an attacker may be
// able to pick the keys in order to avoid potential hash collision (tree
// explosion) attacks.
func UseHashFunction(hash func([]byte) []byte) Option {
	return func(nd *Node) {
		nd.hash = hash
	}
}

//-----------------------------------------------------------------------------
// Instance and helpers functions

// NewNode creates a new IPLD HAMT Node with the given IPLD store and any
// additional options (bitWidth and hash function).
//
// This function creates a new HAMT that you can use directly and is also
// used internally to create child nodes.
func NewNode(cs cbor.IpldStore, options ...Option) *Node {
	nd := &Node{
		Bitfield: big.NewInt(0),
		Pointers: make([]*Pointer, 0),
		store:    cs,
		hash:     defaultHashFunction,
		bitWidth: defaultBitWidth,
	}
	// apply functional options to node before using
	for _, option := range options {
		option(nd)
	}
	return nd
}

// Find navigates through the HAMT structure to where key `k` should exist. If
// the key is not found, an ErrNotFound error is returned. If the key is found
// and the `out` parameter has an UnmarshalCBOR(Reader) method, the decoded
// value is returned. If found and the `out` parameter is `nil`, then `nil`
// will be returned (can be used to determine if a key exists where you don't
// need the value, e.g. using the HAMT as a Set).
//
// Depending on the size of the HAMT, this method may load a large number of
// child nodes via the HAMT's IpldStore.
func (n *Node) Find(ctx context.Context, k string, out interface{}) error {
	return n.getValue(ctx, &hashBits{b: n.hash([]byte(k))}, k, func(kv *KV) error {
		// used to just see if the thing exists in the set
		if out == nil {
			return nil
		}

		if um, ok := out.(cbg.CBORUnmarshaler); ok {
			return um.UnmarshalCBOR(bytes.NewReader(kv.Value.Raw))
		}

		if err := cbor.DecodeInto(kv.Value.Raw, out); err != nil {
			return xerrors.Errorf("cbor decoding value: %w", err)
		}

		return nil
	})
}

// FindRaw performs the same function as Find, but returns the raw bytes found
// at the key's location (which may or may not be DAG-CBOR, see also SetRaw).
func (n *Node) FindRaw(ctx context.Context, k string) ([]byte, error) {
	var ret []byte
	err := n.getValue(ctx, &hashBits{b: n.hash([]byte(k))}, k, func(kv *KV) error {
		ret = kv.Value.Raw
		return nil
	})
	return ret, err
}

// Delete removes an entry entirely from the HAMT structure.
//
// This operation will result in the modification of _at least_ one IPLD block
// via the IpldStore. Depending on the contents of the leaf node, this
// operation may result in a node collapse to shrink the HAMT into its
// canonical form for the remaining data. For an insufficiently random
// collection of keys at the relevant leaf nodes such a collapse may cascade to
// further nodes.
func (n *Node) Delete(ctx context.Context, k string) error {
	kb := []byte(k)
	return n.modifyValue(ctx, &hashBits{b: n.hash(kb)}, kb, nil)
}

// handle the two Find operations in a recursive manner, where each node in the
// HAMT we traverse we call this function again with the same parameters. Note
// that `hv` contains state and `hv.Next()` is not idempotent. Each call
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
		return ErrNotFound
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

	return ErrNotFound
}

// load a HAMT node from the IpldStore and pass on the (assumed) parameters
// that are not stored with the node.
func (p *Pointer) loadChild(ctx context.Context, ns cbor.IpldStore, bitWidth int, hash func([]byte) []byte) (*Node, error) {
	if p.cache != nil {
		return p.cache, nil
	}

	out, err := LoadNode(ctx, ns, p.Link)
	if err != nil {
		return nil, err
	}
	// these don't get set in LoadNode because we don't have the Options
	// at this point but what is inherited from the parents may have varied
	// from the defaults
	out.bitWidth = bitWidth
	out.hash = hash

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
	// TODO(rvagg): loaded nodes must be validated to make sure we have only
	// the correct form of Nodes to avoid attacks from alternative implementations
	// that feed us poorly formed data. Check that:
	// 1. Pointers contains the correct number of elements defined by Bitfield
	// 2. Pointers contain *only* a link or a bucket (this may already be done in
	// the CBOR unmarshal but might be worth doing here so the check is all in
	// one place)
	// 3. Pointers with links have are DAG-CBOR multicodec
	// 4. KV buckets contain strictly between 1 and bucketSize elements
	// 5. KV buckets are ordered by key (bytewise comparison)
	// 6. keys and values are valid (what are the rules? len(key)>0? can val be
	// nul? etc.)
	// 7. .. potentially we could validate the position of elements if we propagate
	// the depth of this node so we know which bits to chomp off the hash digest.
	var out Node
	if err := cs.Get(ctx, c, &out); err != nil {
		return nil, err
	}

	out.store = cs
	out.bitWidth = defaultBitWidth
	out.hash = defaultHashFunction
	// apply functional options to node before using
	for _, option := range options {
		option(&out)
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

// Flush saves and purges any cached Nodes recursively from this Node through
// its (cached) children. Cached nodes primarily exist through the use of
// Copy() operations where the entire graph is instantiated in memory and each
// child pointer exists in cached form.
func (n *Node) Flush(ctx context.Context) error {
	for _, p := range n.Pointers {
		if p.cache != nil {
			if err := p.cache.Flush(ctx); err != nil {
				return err
			}

			c, err := n.store.Put(ctx, p.cache)
			if err != nil {
				return err
			}

			p.cache = nil
			p.Link = c
		}
	}
	return nil
}

// Set key k to value v, where v is has a MarshalCBOR(bytes.Buffer) method to
// encode it.
func (n *Node) Set(ctx context.Context, k string, v interface{}) error {
	var d *cbg.Deferred

	kb := []byte(k)

	cm, ok := v.(cbg.CBORMarshaler)
	if ok {
		buf := new(bytes.Buffer)
		if err := cm.MarshalCBOR(buf); err != nil {
			return err
		}
		d = &cbg.Deferred{Raw: buf.Bytes()}
	} else {
		b, err := cbor.DumpObject(v)
		if err != nil {
			return err
		}
		d = &cbg.Deferred{Raw: b}
	}

	return n.modifyValue(ctx, &hashBits{b: n.hash(kb)}, kb, d)
}

// SetRaw is similar to Set but sets key k in the HAMT to raw bytes without
// performing a DAG-CBOR marshal. The bytes may or may not be encoded DAG-CBOR
// (see also FindRaw for fetching raw form).
func (n *Node) SetRaw(ctx context.Context, k string, raw []byte) error {
	d := &cbg.Deferred{Raw: raw}
	kb := []byte(k)
	return n.modifyValue(ctx, &hashBits{b: n.hash(kb)}, kb, d)
}

// When deleting elements, we need to perform a compaction such that there is
// a single canonical form of any HAMT with a given set of key/value pairs.
// Any node with less than `bucketSize` elements needs to be collapsed into a
// bucket of Pointers in the parent node.
// TODO(rvagg): I don't think the logic here is correct. A compaction should
// occur strictly when: there are no links to child nodes remaining (assuming
// we've cleaned them first and they haven't caused a cascading collapse to
// here) and the number of direct elements (actual k/v pairs) in this node is
// equal o bucketSize+1. Anything less than bucketSize+1 is invalid for a node
// other than the root node (which probably won't have cleanChild() called on
// it). e.g.
// https://github.com/rvagg/iamap/blob/fad95295b013c8b4f0faac6dd5d9be175f6e606c/iamap.js#L333
// If we perform this depth-first, then it's possible to see the collapse
// cascade upward such that we end up with some parent node with a bucket with
// only bucketSize elements. The canonical form of the HAMT requires that
// any node that could be collapsed into a parent bucket is collapsed and.
func (n *Node) cleanChild(chnd *Node, cindex byte) error {
	l := len(chnd.Pointers)
	switch {
	case l == 0:
		return fmt.Errorf("incorrectly formed HAMT")
	case l == 1:
		// TODO: only do this if its a value, cant do this for shards unless pairs requirements are met.

		ps := chnd.Pointers[0]
		if ps.isShard() {
			return nil
		}

		return n.setPointer(cindex, ps)
	case l <= bucketSize:
		var chvals []*KV
		for _, p := range chnd.Pointers {
			if p.isShard() {
				return nil
			}

			for _, sp := range p.KVs {
				if len(chvals) == bucketSize {
					return nil
				}
				chvals = append(chvals, sp)
			}
		}
		return n.setPointer(cindex, &Pointer{KVs: chvals})
	default:
		return nil
	}
}

// Add a new value, update an existing value, or delete a value from the HAMT,
// potentially recursively calling child nodes to find the exact location of
// the entry in question and potentially collapsing nodes into buckets in
// parent nodes where a deletion violates the canonical form rules (see
// cleanNode()). Recursive calls use the same arguments on child nodes but
// note that `hv.Next()` is not idempotent. Each call will increment the number
// of bits chomped off the hash digest for this key.
func (n *Node) modifyValue(ctx context.Context, hv *hashBits, k []byte, v *cbg.Deferred) error {
	idx, err := hv.Next(n.bitWidth)
	if err != nil {
		return ErrMaxDepth
	}

	// if the element expected at this node isn't here then we can be sure it
	// doesn't exist in the HAMT already and can insert it at the appropriate
	// position.
	if n.Bitfield.Bit(idx) != 1 {
		return n.insertKV(idx, k, v)
	}

	// otherwise, the value is either local or in a child

	// perform a popcount of bits up to the `idx` to find `cindex`
	cindex := byte(n.indexForBitPos(idx))

	child := n.getPointer(cindex)
	if child.isShard() {
		// if isShard, we have a pointer to a child that we need to load and
		// delegate our modify operation to
		chnd, err := child.loadChild(ctx, n.store, n.bitWidth, n.hash)
		if err != nil {
			return err
		}

		if err := chnd.modifyValue(ctx, hv, k, v); err != nil {
			return err
		}

		// CHAMP optimization, ensure the HAMT retains its canonical form for the
		// current data it contains. This may involve collapsing child nodes if
		// they no longer contain enough elements to justify their stand-alone
		// existence.
		if v == nil {
			if err := n.cleanChild(chnd, cindex); err != nil {
				return err
			}
		}

		return nil
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
					return n.rmPointer(cindex, idx)
				}

				copy(child.KVs[i:], child.KVs[i+1:])
				child.KVs = child.KVs[:len(child.KVs)-1]
				return nil
			}
		}
		return ErrNotFound
	}

	// modify existing, check if key already exists
	for _, p := range child.KVs {
		if bytes.Equal(p.Key, k) {
			p.Value = v
			return nil
		}
	}

	if len(child.KVs) >= bucketSize {
		// bucket is full, create a child node (shard) with all existing bucket
		// elements plus the new one and set it in the place of the bucket
		// TODO(rvagg): this all of the modifyValue() calls are going to result
		// in a store.Put(), this could be improved by allowing NewNode() to take
		// the bulk set of elements, or modifying modifyValue() for the case
		// where we know for sure that the elements will go into buckets and
		// not cause an overflow - i.e. we just need to take each element, hash it
		// and consume the correct number of bytes off the digest and figure out
		// where it should be in the new node.
		sub := NewNode(n.store)
		sub.bitWidth = n.bitWidth
		sub.hash = n.hash
		hvcopy := &hashBits{b: hv.b, consumed: hv.consumed}
		if err := sub.modifyValue(ctx, hvcopy, k, v); err != nil {
			return err
		}

		for _, p := range child.KVs {
			chhv := &hashBits{b: n.hash([]byte(p.Key)), consumed: hv.consumed}
			if err := sub.modifyValue(ctx, chhv, p.Key, p.Value); err != nil {
				return err
			}
		}

		c, err := n.store.Put(ctx, sub)
		if err != nil {
			return err
		}

		return n.setPointer(cindex, &Pointer{Link: c})
	}

	// otherwise insert the new element into the array in order, the ordering is
	// important to retain canonical form
	np := &KV{Key: k, Value: v}
	for i := 0; i < len(child.KVs); i++ {
		if bytes.Compare(k, child.KVs[i].Key) < 0 {
			child.KVs = append(child.KVs[:i], append([]*KV{np}, child.KVs[i:]...)...)
			return nil
		}
	}
	child.KVs = append(child.KVs, np)
	return nil
}

// Insert a new key/value pair into the current node at the specified index.
// This will involve modifying the bitfield for that index and inserting a new
// bucket containing the single key/value pair at that position.
func (n *Node) insertKV(idx int, k []byte, v *cbg.Deferred) error {
	if v == nil {
		return ErrNotFound
	}

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
	nn := NewNode(n.store)
	nn.bitWidth = n.bitWidth
	nn.hash = n.hash
	nn.Bitfield.Set(n.Bitfield)
	nn.Pointers = make([]*Pointer, len(n.Pointers))

	for i, p := range n.Pointers {
		pp := &Pointer{}
		if p.cache != nil {
			pp.cache = p.cache.Copy()
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
	return p.Link.Defined()
}

// ForEach recursively calls function f on each k / val pair found in the HAMT.
// This performs a full traversal of the graph and for large HAMTs can cause
// a large number of loads from the IpldStore. This should not be used lightly
// as it can incur large costs.
func (n *Node) ForEach(ctx context.Context, f func(k string, val interface{}) error) error {
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
