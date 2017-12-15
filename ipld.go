package hamt

import (
	"context"
	"math"
	"time"

	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	bserv "github.com/ipfs/go-ipfs/blockservice"
	offline "github.com/ipfs/go-ipfs/exchange/offline"

	cbor "github.com/ipfs/go-ipld-cbor"
	atlas "github.com/polydawn/refmt/obj/atlas"
	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	ds "gx/ipfs/QmdHG8MAuARdGHxx4rPQASLcvhz24fzjSQq7AJRAQEorq5/go-datastore"
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
	BlockService bserv.BlockService
}

func NewCborStore() *CborIpldStore {
	bs := bstore.NewBlockstore(ds.NewMapDatastore())
	return &CborIpldStore{bserv.New(bs, offline.Exchange(bs))}
}

func (s *CborIpldStore) Get(ctx context.Context, c *cid.Cid, out interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	blk, err := s.BlockService.GetBlock(ctx, c)
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

	c, err := s.BlockService.AddBlock(nd)
	if err != nil {
		return nil, err
	}

	return c, nil
}
