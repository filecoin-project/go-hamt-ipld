package hamt

import (
	"bytes"
	"fmt"
	"io"
	"math/big"

	block "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// Reify looks at an ipld Node and tries to interpret it as a HAMT with default options.
// if successful, it returns the HAMT
func Reify(lnkCtx ipld.LinkContext, maybeHamt ipld.Node, lsys *ipld.LinkSystem) (ipld.Node, error) {
	rf := MakeReifier()
	return rf(lnkCtx, maybeHamt, lsys)
}

// MakeReifier allows generation of a Reify function with an explicit hash / bitwidth.
func MakeReifier(opts ...Option) ipld.NodeReifier {
	return func(lnkCtx ipld.LinkContext, maybeHamt ipld.Node, lsys *ipld.LinkSystem) (ipld.Node, error) {
		if maybeHamt.Kind() != ipld.Kind_List {
			return maybeHamt, nil
		}

		// per Node layout:
		li := maybeHamt.ListIterator()
		idx, bitField, err := li.Next()
		if err != nil || idx != 0 {
			return maybeHamt, nil
		}
		bitFieldBytes, err := bitField.AsBytes()
		if err != nil {
			return maybeHamt, nil
		}
		idx, pointers, err := li.Next()
		if err != nil || idx != 1 {
			return maybeHamt, nil
		}
		if pointers.Kind() != ipld.Kind_List {
			return maybeHamt, nil
		}
		if li.Done() != true {
			return maybeHamt, nil
		}
		ptrs := make([]*Pointer, 0)
		pti := pointers.ListIterator()
		for !pti.Done() {
			_, p, err := pti.Next()
			if err != nil {
				return maybeHamt, nil
			}
			ptr, err := loadPointer(p)
			if err != nil {
				return maybeHamt, nil
			}
			ptrs = append(ptrs, ptr)
		}

		bs := LSBlockstore{lsys}
		store := cbor.NewCborStore(&bs)
		bfi := big.NewInt(0).SetBytes(bitFieldBytes)

		hn := &Node{
			Bitfield: bfi,
			Pointers: ptrs,
			bitWidth: defaultBitWidth,
			hash:     defaultHashFunction,
			store:    store,
			proto:    basicnode.Prototype.Map,
		}
		for _, o := range opts {
			o(hn)
		}

		return hn, nil
	}
}

func loadPointer(p ipld.Node) (*Pointer, error) {
	if p.Kind() != ipld.Kind_Map {
		return nil, fmt.Errorf("invalid pointer kind")
	}
	pm := p.MapIterator()
	k, v, err := pm.Next()
	if err != nil {
		return nil, fmt.Errorf("no pointer map key")
	}
	ks, err := k.AsString()
	if err != nil {
		return nil, fmt.Errorf("pointer map key not string")
	}
	// this is a link.
	if ks == string(keyZero) {
		lnk, err := v.AsLink()
		if err != nil {
			return nil, fmt.Errorf("pointer union indicates link, but not link")
		}
		return &Pointer{Link: lnk.(cidlink.Link).Cid}, nil
		// this is an array of kvs
	} else if ks == string(keyOne) {
		kvs := make([]*KV, 0)
		li := v.ListIterator()
		for !li.Done() {
			_, kvn, err := li.Next()
			if err != nil {
				return nil, fmt.Errorf("failed to load list item: %w", err)
			}
			kv, err := loadKV(kvn)
			if err != nil {
				return nil, err
			}
			kvs = append(kvs, kv)
		}
		return &Pointer{KVs: kvs}, nil
	} else {
		return nil, fmt.Errorf("invalid pointer union signal")
	}
}

func loadKV(n ipld.Node) (*KV, error) {
	if n.Kind() != ipld.Kind_List {
		return nil, fmt.Errorf("kv should be of kind list")
	}
	li := n.ListIterator()
	_, k, err := li.Next()
	if err != nil {
		return nil, err
	}
	kb, err := k.AsBytes()
	if err != nil {
		return nil, err
	}
	_, v, err := li.Next()
	if err != nil {
		return nil, err
	}
	if !li.Done() {
		return nil, fmt.Errorf("kv did not have 2 entries as expected")
	}

	buf := bytes.NewBuffer(nil)
	if err := dagcbor.Encode(v, buf); err != nil {
		return nil, err
	}

	return &KV{
		Key:   kb,
		Value: &cbg.Deferred{Raw: buf.Bytes()},
	}, nil
}

// LSBlockstore creates a blockstore (get/put) interface over a link system
type LSBlockstore struct {
	*ipld.LinkSystem
}

// Get a raw block of data from a cid using a link system storage read opener
func (l *LSBlockstore) Get(c cid.Cid) (block.Block, error) {
	rdr, err := l.StorageReadOpener(linking.LinkContext{}, cidlink.Link{Cid: c})
	if err != nil {
		return nil, err
	}
	bytes, err := io.ReadAll(rdr)
	if err != nil {
		return nil, err
	}
	return block.NewBlockWithCid(bytes, c)
}

// Put a block of data to an underlying store using a link system storage write opener
func (l *LSBlockstore) Put(b block.Block) error {
	w, committer, err := l.LinkSystem.StorageWriteOpener(linking.LinkContext{})
	if err != nil {
		return err
	}
	if _, err := w.Write(b.RawData()); err != nil {
		return err
	}
	return committer(cidlink.Link{Cid: b.Cid()})
}
