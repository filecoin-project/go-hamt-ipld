package hamt

import (
	"context"
	"math"

	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	atlas "github.com/polydawn/refmt/obj/atlas"
	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

// THIS IS ALL TEMPORARY CODE

func init() {
	cbor.RegisterCborType(Node{})
	cbor.RegisterCborType(Pointer{})

	kvAtlasEntry := atlas.BuildEntry(KV{}).Transform().TransformMarshal(
		atlas.MakeMarshalTransformFunc(func(kv KV) ([]string, error) {
			return []string{kv.Key, kv.Value}, nil
		})).TransformUnmarshal(
		atlas.MakeUnmarshalTransformFunc(func(v []string) (KV, error) {
			return KV{
				Key:   v[0],
				Value: v[1],
			}, nil
		})).Complete()
	cbor.RegisterCborType(kvAtlasEntry)
}

type CborIpldStore struct {
	bs bstore.Blockstore
}

func NewCborStore() *CborIpldStore {
	return &CborIpldStore{bstore.NewBlockstore(ds.NewMapDatastore())}
}

func (s *CborIpldStore) Get(ctx context.Context, c *cid.Cid, out interface{}) error {
	blk, err := s.bs.Get(c)
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

	if err := s.bs.Put(nd); err != nil {
		return nil, err
	}

	return nd.Cid(), nil
}
