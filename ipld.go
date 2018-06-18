package hamt

import (
	"context"
	"math"
	"time"

	/*
		bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
		bserv "github.com/ipfs/go-ipfs/blockservice"
		offline "github.com/ipfs/go-ipfs/exchange/offline"
	*/

	block "github.com/ipfs/go-block-format"
	cbor "github.com/ipfs/go-ipld-cbor"
	atlas "github.com/polydawn/refmt/obj/atlas"
	//ds "gx/ipfs/QmdHG8MAuARdGHxx4rPQASLcvhz24fzjSQq7AJRAQEorq5/go-datastore"
	cid "github.com/ipfs/go-cid"
)

// THIS IS ALL TEMPORARY CODE

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
				Value: v[1].([]byte),
			}, nil
		})).Complete()
	cbor.RegisterCborType(kvAtlasEntry)
}

type CborIpldStore struct {
	Blocks blocks
}

type blocks interface {
	GetBlock(context.Context, *cid.Cid) (block.Block, error)
	AddBlock(block.Block) error
}

type mockBlocks struct {
	data map[string][]byte
}

func newMockBlocks() *mockBlocks {
	return &mockBlocks{make(map[string][]byte)}
}

func (mb *mockBlocks) GetBlock(ctx context.Context, cid *cid.Cid) (block.Block, error) {
	d, ok := mb.data[cid.KeyString()]
	if ok {
		return block.NewBlock(d), nil
	}
	return nil, ErrNotFound
}

func (mb *mockBlocks) AddBlock(b block.Block) error {
	mb.data[b.Cid().KeyString()] = b.RawData()
	return nil
}

func NewCborStore() *CborIpldStore {
	return &CborIpldStore{newMockBlocks()}
}

func (s *CborIpldStore) Get(ctx context.Context, c *cid.Cid, out interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	blk, err := s.Blocks.GetBlock(ctx, c)
	if err != nil {
		return err
	}
	return cbor.DecodeInto(blk.RawData(), out)
}

var puts int

type cidProvider interface {
	Cid() *cid.Cid
}

func (s *CborIpldStore) Put(ctx context.Context, v interface{}) (*cid.Cid, error) {
	puts++

	mhType := uint64(math.MaxUint64)
	mhLen := -1
	if c, ok := v.(cidProvider); ok {
		pref := c.Cid().Prefix()
		mhType = pref.MhType
		mhLen = pref.MhLength
	}

	nd, err := cbor.WrapObject(v, mhType, mhLen)
	if err != nil {
		return nil, err
	}

	if err := s.Blocks.AddBlock(nd); err != nil {
		return nil, err
	}

	return nd.Cid(), nil
}
