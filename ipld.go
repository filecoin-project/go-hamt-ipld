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

	atlas "gx/ipfs/QmSaDQWMxJBMtzQWnGoDppbwSEbHv4aJcD86CMSdszPU4L/refmt/obj/atlas"
	cbor "gx/ipfs/QmZpue627xQuNGXn7xHieSjSZ8N4jot6oBHwe9XTn3e4NU/go-ipld-cbor"
	block "gx/ipfs/Qmej7nf81hi2x2tvjRBF3mcp74sQyuDH4VMYDGd1YtXjb2/go-block-format"
	//ds "gx/ipfs/QmdHG8MAuARdGHxx4rPQASLcvhz24fzjSQq7AJRAQEorq5/go-datastore"
	cid "gx/ipfs/QmcZfnkapfECQGcLZaf9B79NRg7cRa9EnZh4LSbkCzwNvY/go-cid"
)

// THIS IS ALL TEMPORARY CODE

func init() {
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
	AddBlock(block.Block) (*cid.Cid, error)
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

func (mb *mockBlocks) AddBlock(b block.Block) (*cid.Cid, error) {
	mb.data[b.Cid().KeyString()] = b.RawData()
	return b.Cid(), nil
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

func (s *CborIpldStore) Put(ctx context.Context, v interface{}) (*cid.Cid, error) {
	puts++

	nd, err := cbor.WrapObject(v, math.MaxUint64, -1)
	if err != nil {
		return nil, err
	}

	c, err := s.Blocks.AddBlock(nd)
	if err != nil {
		return nil, err
	}

	return c, nil
}
