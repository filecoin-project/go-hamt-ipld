package hamt

import (
	"bytes"
	"context"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
)

var _ ipld.Node = (*Node)(nil)

func (n *Node) AsBool() (bool, error) {
	return false, ErrNotFound
}

func (n *Node) AsBytes() ([]byte, error) {
	return nil, ErrNotFound
}

func (n *Node) AsString() (string, error) {
	return "", ErrNotFound
}

func (n *Node) AsInt() (int, error) {
	return 0, ErrNotFound
}

func (n *Node) AsFloat() (float64, error) {
	return 0.0, ErrNotFound
}

func (n *Node) AsLink() (ipld.Link, error) {
	return nil, ErrNotFound
}

func (n *Node) IsAbsent() bool {
	return false
}

func (n *Node) IsNull() bool {
	return n.Bitfield == nil
}

func (n *Node) Length() int {
	l := 0
	for _, p := range n.Pointers {
		if p.Link.Defined() {
			c, err := p.loadChild(context.Background(), n.store, n.bitWidth, n.hash, n.proto)
			if err != nil {
				return -1
			}
			l += c.Length()
		} else {
			l += len(p.KVs)
		}
	}
	return l
}

func (n *Node) ReprKind() ipld.ReprKind {
	return ipld.ReprKind_Map
}

// LookupByString looks up a child object in this node and returns it.
// The returned Node may be any of the ReprKind:
// a primitive (string, int, etc), a map, a list, or a link.
//
// If the Kind of this Node is not ReprKind_Map, a nil node and an error
// will be returned.
//
// If the key does not exist, a nil node and an error will be returned.
func (n *Node) LookupByString(key string) (ipld.Node, error) {
	data, err := n.FindRaw(context.Background(), key)
	if err != nil {
		return nil, err
	}
	_, val, err := n.realize(key, data)
	return val, err
}

func (n *Node) LookupByNode(key ipld.Node) (ipld.Node, error) {
	if key.ReprKind() == ipld.ReprKind_String {
		s, e := key.AsString()
		if e != nil {
			return nil, e
		}
		return n.LookupByString(s)
	} else if key.ReprKind() == ipld.ReprKind_Bytes {
		b, e := key.AsBytes()
		if e != nil {
			return nil, e
		}
		return n.LookupByString(string(b))
	}
	return nil, ipld.ErrInvalidKey{}
}

func (n *Node) LookupByIndex(idx int) (ipld.Node, error) {
	return nil, ErrNotFound
}

func (n *Node) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	return n.LookupByString(seg.String())
}

// MapIterator returns an iterator which yields key-value pairs
// traversing the node.
// If the node kind is anything other than a map, nil will be returned.
//
// The iterator will yield every entry in the map; that is, it
// can be expected that itr.Next will be called node.Length times
// before itr.Done becomes true.
func (n *Node) MapIterator() ipld.MapIterator {
	mi := &hmi{
		at:  n,
		ukv: make([]*KV, 0),
		up:  n.Pointers,
	}
	return mi
}

type hmi struct {
	at  *Node
	ukv []*KV
	up  []*Pointer
	err error
}

func (mi *hmi) Done() bool {
	if len(mi.ukv) == 0 && len(mi.up) == 0 {
		return true
	}
	if len(mi.ukv) > 0 {
		return false
	}
	mi.loadNext()
	return mi.Done()
}

func (mi *hmi) loadNext() {
	p := mi.up[0]
	mi.up = mi.up[1:]
	if p.isShard() {
		chld, err := p.loadChild(context.Background(), mi.at.store, mi.at.bitWidth, mi.at.hash, mi.at.proto)
		if err != nil {
			mi.err = err
			return
		}
		mi.up = append(mi.up, chld.Pointers...)
	} else {
		mi.ukv = append(mi.ukv, p.KVs...)
	}
}

func (mi *hmi) Next() (ipld.Node, ipld.Node, error) {
	if mi.err != nil {
		return nil, nil, mi.err
	}
	// If false, we've ensured at least one entry in mi.ukv
	if mi.Done() {
		return nil, nil, mi.err
	}

	kv := mi.ukv[0]
	mi.ukv = mi.ukv[1:]
	return mi.at.realize(string(kv.Key), kv.Value.Raw)
}

func (n *Node) realize(key string, value []byte) (ipld.Node, ipld.Node, error) {
	ma, err := n.proto.NewBuilder().BeginMap(0)
	if err != nil {
		return nil, nil, err
	}

	mak := ma.KeyPrototype()
	mav := ma.ValuePrototype(key)

	keyBuilder := mak.NewBuilder()
	if err := keyBuilder.AssignString(key); err != nil {
		return nil, nil, err
	}

	valueBuilder := mav.NewBuilder()
	if err := dagcbor.Decoder(valueBuilder, bytes.NewBuffer(value)); err != nil {
		return nil, nil, err
	}
	return keyBuilder.Build(), valueBuilder.Build(), nil
}

func (n *Node) ListIterator() ipld.ListIterator {
	return nil
}

func (n *Node) Prototype() ipld.NodePrototype {
	return n.proto
}

func NewTypedHamt(key ipld.NodePrototype, value ipld.NodePrototype) ipld.NodePrototype {
	return &hamtProto{key, value}
}

type hamtProto struct {
	k ipld.NodePrototype
	v ipld.NodePrototype
}

func (h *hamtProto) NewBuilder() ipld.NodeBuilder {
	return &hamtBuilder{h, nil}
}

type hamtBuilder struct {
	proto *hamtProto

	n *Node
}

func (h *hamtBuilder) Build() ipld.Node {
	return h.n
}

func (h *hamtBuilder) Reset() {
	h.n = nil
}

func (h *hamtBuilder) BeginMap(sizeHint int) (ipld.MapAssembler, error) {
	return h, nil
}

func (h *hamtBuilder) AssembleKey() ipld.NodeAssembler {
	return nil
}

func (h *hamtBuilder) AssembleValue() ipld.NodeAssembler {
	return nil
}

func (h *hamtBuilder) AssembleEntry(k string) (ipld.NodeAssembler, error) {
	return nil, nil
}

func (h *hamtBuilder) Finish() error {
	return nil
}
func (h *hamtBuilder) KeyPrototype() ipld.NodePrototype {
	return h.proto.k
}
func (h *hamtBuilder) ValuePrototype(k string) ipld.NodePrototype {
	return h.proto.v
}

func (h *hamtBuilder) BeginList(sizeHint int) (ipld.ListAssembler, error) {
	return nil, ErrNotFound
}
func (h *hamtBuilder) AssignNull() error {
	return ErrNotFound
}
func (h *hamtBuilder) AssignBool(bool) error {
	return ErrNotFound
}
func (h *hamtBuilder) AssignInt(int) error {
	return ErrNotFound
}
func (h *hamtBuilder) AssignFloat(float64) error {
	return ErrNotFound
}
func (h *hamtBuilder) AssignString(string) error {
	return ErrNotFound
}
func (h *hamtBuilder) AssignBytes([]byte) error {
	return ErrNotFound
}
func (h *hamtBuilder) AssignLink(ipld.Link) error {
	return ErrNotFound
}

func (h *hamtBuilder) AssignNode(n ipld.Node) error {
	hn, ok := n.(*Node)
	if !ok {
		return ErrMalformedHamt
	}
	h.n = hn
	return nil
}

func (h *hamtBuilder) Prototype() ipld.NodePrototype {
	return h.proto
}
