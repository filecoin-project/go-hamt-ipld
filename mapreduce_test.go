package hamt

import (
	"context"
	"slices"
	"strings"
	"testing"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
)

type readCounterStore struct {
	cbor.IpldStore
	readCount int
}

func (rcs *readCounterStore) Get(ctx context.Context, c cid.Cid, out any) error {
	rcs.readCount++
	return rcs.IpldStore.Get(ctx, c, out)
}

func TestMapReduceSimple(t *testing.T) {
	ctx := context.Background()
	opts := []Option{UseTreeBitWidth(5)}
	cs := &readCounterStore{cbor.NewCborStore(newMockBlocks()), 0}

	N := 50000
	var rootCid cid.Cid
	golden := make(map[string]string)
	{
		begn, err := NewNode(cs, opts...)
		require.NoError(t, err)

		for range N {
			k := randKey()
			v := randValue()
			golden[k] = string([]byte(*v))
			begn.Set(ctx, k, v)
		}

		rootCid, err = begn.Write(ctx)
		require.NoError(t, err)
	}

	type kv struct {
		k string
		v string
	}

	mapper := func(k string, v CborByteArray) ([]kv, error) {
		return []kv{{k, string([]byte(v))}}, nil
	}
	reducer := func(kvs [][]kv) ([]kv, error) {
		var kvsConcat []kv
		for _, kvs := range kvs {
			kvsConcat = append(kvsConcat, kvs...)
		}
		slices.SortFunc(kvsConcat, func(a, b kv) int {
			return strings.Compare(a.k, b.k)
		})
		return kvsConcat, nil
	}

	cmr, err := NewCachedMapReduce(mapper, reducer, 200)
	t.Logf("tree size: %d, cache size: %d", N, cmr.cache.cacheSize)
	require.NoError(t, err)

	cs.readCount = 0
	res, err := cmr.MapReduce(ctx, cs, rootCid, opts...)
	require.NoError(t, err)
	require.Equal(t, len(golden), len(res))
	t.Logf("fresh readCount: %d", cs.readCount)

	cs.readCount = 0
	res, err = cmr.MapReduce(ctx, cs, rootCid, opts...)
	require.NoError(t, err)
	t.Logf("fresh re-readCount: %d", cs.readCount)
	require.Less(t, cs.readCount, 200)

	verifyConsistency := func(res []kv) {
		t.Helper()
		mappedRes := make(map[string]string)
		for _, kv := range res {
			mappedRes[kv.k] = kv.v
		}
		require.Equal(t, len(golden), len(mappedRes))
		require.Equal(t, golden, mappedRes)
	}
	verifyConsistency(res)

	{
		begn, err := LoadNode(ctx, cs, rootCid, opts...)
		require.NoError(t, err)
		// add new key
		k := randKey()
		v := randValue()
		golden[k] = string([]byte(*v))
		begn.Set(ctx, k, v)

		rootCid, err = begn.Write(ctx)
		require.NoError(t, err)
	}

	cs.readCount = 0
	res, err = cmr.MapReduce(ctx, cs, rootCid, opts...)
	require.NoError(t, err)
	verifyConsistency(res)
	t.Logf("new key readCount: %d", cs.readCount)
	require.Less(t, cs.readCount, 200)

	cs.readCount = 0
	res, err = cmr.MapReduce(ctx, cs, rootCid, opts...)
	require.NoError(t, err)
	verifyConsistency(res)
	t.Logf("repeat readCount: %d", cs.readCount)
	require.Less(t, cs.readCount, 200)

	cs.readCount = 0
	res, err = cmr.MapReduce(ctx, cs, rootCid, opts...)
	require.NoError(t, err)
	verifyConsistency(res)
	t.Logf("repeat readCount: %d", cs.readCount)
	require.Less(t, cs.readCount, 200)

	{
		begn, err := LoadNode(ctx, cs, rootCid, opts...)
		require.NoError(t, err)
		// add two new keys
		k := randKey()
		v := randValue()
		golden[k] = string([]byte(*v))
		begn.Set(ctx, k, v)
		k = randKey()
		v = randValue()
		golden[k] = string([]byte(*v))
		begn.Set(ctx, k, v)

		rootCid, err = begn.Write(ctx)
		require.NoError(t, err)
	}

	cs.readCount = 0
	res, err = cmr.MapReduce(ctx, cs, rootCid, opts...)
	require.NoError(t, err)
	verifyConsistency(res)
	t.Logf("new two keys readCount: %d", cs.readCount)
	require.Less(t, cs.readCount, 300)
}
