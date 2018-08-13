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

	cbor "gx/ipfs/QmSyK1ZiAP98YvnxsTfQpb669V2xeTHRbG4Y6fgKS3vVSd/go-ipld-cbor"
	block "gx/ipfs/QmVzK524a2VWLqyvtBeiHKsUAWYgeAk4DBeZoY7vpNPNRx/go-block-format"
	atlas "gx/ipfs/QmcrriCMhjb5ZWzmPNxmP53px47tSPcXBNaMtLdgcKFJYk/refmt/obj/atlas"
	//ds "gx/ipfs/QmdHG8MAuARdGHxx4rPQASLcvhz24fzjSQq7AJRAQEorq5/go-datastore"
	cid "gx/ipfs/QmYVNvtQkeZ6AKSwDrjQTs432QtL6umrrK41EBq3cu7iSP/go-cid"
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

type cidProvider interface {
	Cid() *cid.Cid
}

func (s *CborIpldStore) Put(ctx context.Context, v interface{}) (*cid.Cid, error) {
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
