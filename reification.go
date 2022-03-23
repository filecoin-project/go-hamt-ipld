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

		cfg := defaultConfig()
		for _, o := range opts {
			o(cfg)
		}
		hn := newNode(store, cfg.hashFn, cfg.bitWidth, cfg.proto)
		hn.Bitfield = bfi
		hn.Pointers = ptrs

		return hn, nil
	}
}

func loadPointer(p ipld.Node) (*Pointer, error) {
	if p.Kind() != ipld.Kind_Link {
		lnk, err := p.AsLink()
		if err != nil {
			return nil, fmt.Errorf("pointer union indicates link, but not link")
		}
		return &Pointer{Link: lnk.(cidlink.Link).Cid}, nil
	} else if p.Kind() == ipld.Kind_List {
		kvs := make([]*KV, 0)
		li := p.ListIterator()
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
	}
	return nil, fmt.Errorf("unsupported pointer kind")
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
