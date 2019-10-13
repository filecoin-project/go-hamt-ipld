package hamt

import (
	"bytes"
	"context"
	"fmt"
	"time"

	/*
		bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
				bserv "github.com/ipfs/go-ipfs/blockservice"
				offline "github.com/ipfs/go-ipfs/exchange/offline"
	*/

	block "github.com/ipfs/go-block-format"
	cbor "github.com/ipfs/go-ipld-cbor"
	recbor "github.com/polydawn/refmt/cbor"
	atlas "github.com/polydawn/refmt/obj/atlas"

	//ds "gx/ipfs/QmdHG8MAuARdGHxx4rPQASLcvhz24fzjSQq7AJRAQEorq5/go-datastore"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// THIS IS ALL TEMPORARY CODE

/*
func init() {
	cbor.RegisterCborType(cbor.BigIntAtlasEntry)
	cbor.RegisterCborType(Node{})
	cbor.RegisterCborType(Pointer{})

	kvAtlasEntry := atlas.BuildEntry(KV{}).Transform().TransformMarshal(
		atlas.MakeMarshalTransformFunc(func(kv KV) ([]interface{}, error) {
			return []interface{}{kv.Key, kv.Value}, nil
		})).TransformUnmarshal(
		atlas.MakeUnmarshalTransformFunc(func(v []interface{}) (KV, error) {
			return KV{
				Key:   v[0].(string),
				Value: v[1],
			}, nil
		})).Complete()
	cbor.RegisterCborType(kvAtlasEntry)
}
*/

// CborIpldStore is the ipld store interface required by the hamt.
type CborIpldStore interface {
	Get(ctx context.Context, c cid.Cid, out interface{}) error
	Put(ctx context.Context, v interface{}) (cid.Cid, error)
	GetBlock(context.Context, cid.Cid) (block.Block, error)
}

// BasicCborIpldStore is a simple CborIpldStore made from wrapping a blockstore.
type BasicCborIpldStore struct {
	Blocks blocks
	Atlas  *atlas.Atlas
}

var _ CborIpldStore = &BasicCborIpldStore{}

type blocks interface {
	GetBlock(context.Context, cid.Cid) (block.Block, error)
	AddBlock(block.Block) error
}

type Blockstore interface {
	Get(cid.Cid) (block.Block, error)
	Put(block.Block) error
}

type bswrapper struct {
	bs Blockstore
}

func (bs *bswrapper) GetBlock(_ context.Context, c cid.Cid) (block.Block, error) {
	return bs.bs.Get(c)
}

func (bs *bswrapper) AddBlock(blk block.Block) error {
	return bs.bs.Put(blk)
}

func CSTFromBstore(bs Blockstore) *BasicCborIpldStore {
	return &BasicCborIpldStore{
		Blocks: &bswrapper{bs},
	}
}

type mockBlocks struct {
	data map[cid.Cid][]byte
}

func newMockBlocks() *mockBlocks {
	return &mockBlocks{make(map[cid.Cid][]byte)}
}

func (mb *mockBlocks) GetBlock(ctx context.Context, cid cid.Cid) (block.Block, error) {
	d, ok := mb.data[cid]
	if ok {
		return block.NewBlock(d), nil
	}
	return nil, ErrNotFound
}

func (mb *mockBlocks) AddBlock(b block.Block) error {
	mb.data[b.Cid()] = b.RawData()
	return nil
}

func NewCborStore() *BasicCborIpldStore {
	return &BasicCborIpldStore{Blocks: newMockBlocks()}
}

func (s *BasicCborIpldStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	blk, err := s.Blocks.GetBlock(ctx, c)
	if err != nil {
		return err
	}

	cu, ok := out.(cbg.CBORUnmarshaler)
	if ok {
		return cu.UnmarshalCBOR(bytes.NewReader(blk.RawData()))
	}

	if s.Atlas == nil {
		return cbor.DecodeInto(blk.RawData(), out)
	} else {
		return recbor.UnmarshalAtlased(recbor.DecodeOptions{}, blk.RawData(), out, *s.Atlas)
	}
}

type cidProvider interface {
	Cid() cid.Cid
}

func (s *BasicCborIpldStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	mhType := uint64(mh.BLAKE2B_MIN + 31)
	mhLen := -1
	codec := uint64(cid.DagCBOR)

	var expCid cid.Cid
	if c, ok := v.(cidProvider); ok {
		pref := c.Cid().Prefix()
		mhType = pref.MhType
		mhLen = pref.MhLength
		codec = pref.Codec
		expCid = c.Cid()
	}

	cm, ok := v.(cbg.CBORMarshaler)
	if ok {
		buf := new(bytes.Buffer)
		if err := cm.MarshalCBOR(buf); err != nil {
			return cid.Undef, err
		}

		pref := cid.Prefix{
			Codec:    codec,
			MhType:   mhType,
			MhLength: mhLen,
			Version:  1,
		}
		c, err := pref.Sum(buf.Bytes())
		if err != nil {
			return cid.Undef, err
		}

		blk, err := block.NewBlockWithCid(buf.Bytes(), c)
		if err != nil {
			return cid.Undef, err
		}

		if err := s.Blocks.AddBlock(blk); err != nil {
			return cid.Undef, err
		}

		return blk.Cid(), nil
	}

	nd, err := cbor.WrapObject(v, mhType, mhLen)
	if err != nil {
		return cid.Undef, err
	}

	if err := s.Blocks.AddBlock(nd); err != nil {
		return cid.Undef, err
	}

	if expCid != cid.Undef && nd.Cid() != expCid {
		return cid.Undef, fmt.Errorf("your object is not being serialized the way it expects to")
	}

	return nd.Cid(), nil
}

func (s *BasicCborIpldStore) GetBlock(ctx context.Context, cid cid.Cid) (block.Block, error) {
	return s.Blocks.GetBlock(ctx, cid)
}
