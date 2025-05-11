package hamt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"

	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type cacheEntry[T any] struct {
	value  T
	weight int
}
type weigthted2RCache[T any] struct {
	lk        sync.Mutex
	cache     map[cid.Cid]cacheEntry[T]
	cacheSize int
}

func newWeighted2RCache[T any](cacheSize int) *weigthted2RCache[T] {
	return &weigthted2RCache[T]{
		cache:     make(map[cid.Cid]cacheEntry[T]),
		cacheSize: cacheSize,
	}
}
func (c *weigthted2RCache[T]) Get(k cid.Cid) (cacheEntry[T], bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	v, ok := c.cache[k]
	if !ok {
		return v, false
	}
	return v, true
}

func (c *weigthted2RCache[T]) Add(k cid.Cid, v cacheEntry[T]) {
	// dont cache nodes that require less than 6 reads
	if v.weight <= 5 {
		return
	}
	c.lk.Lock()
	defer c.lk.Unlock()
	if _, ok := c.cache[k]; ok {
		c.cache[k] = v
		return
	}

	c.cache[k] = v
	if len(c.cache) > c.cacheSize {
		// pick two random entris using map iteration
		// work well for cacheSize > 8
		var k1, k2 cid.Cid
		var v1, v2 cacheEntry[T]
		for k, v := range c.cache {
			k1 = k
			v1 = v
			break
		}
		for k, v := range c.cache {
			k2 = k
			v2 = v
			break
		}
		// pick random one based on weight
		r1 := rand.Float64()
		if r1 < float64(v1.weight)/float64(v1.weight+v2.weight) {
			delete(c.cache, k2)
		} else {
			delete(c.cache, k1)
		}
	}
}

// CachedMapReduce is a map reduce implementation that caches intermediate results
// to reduce the number of reads from the underlying store.
type CachedMapReduce[T any, PT interface {
	*T
	cbg.CBORUnmarshaler
}, U any] struct {
	mapper  func(string, T) (U, error)
	reducer func([]U) (U, error)
	cache   *weigthted2RCache[U]
}

// NewCachedMapReduce creates a new CachedMapReduce instance.
// The mapper translates a key-value pair stored in the HAMT into a chosen U value.
// The reducer reduces the U values into a single U value.
// The cacheSize parameter specifies the maximum number of intermediate results to cache.
func NewCachedMapReduce[T any, PT interface {
	*T
	cbg.CBORUnmarshaler
}, U any](
	mapper func(string, T) (U, error),
	reducer func([]U) (U, error),
	cacheSize int,
) (*CachedMapReduce[T, PT, U], error) {
	return &CachedMapReduce[T, PT, U]{
		mapper:  mapper,
		reducer: reducer,
		cache:   newWeighted2RCache[U](cacheSize),
	}, nil
}

// MapReduce applies the map reduce function to the given root node.
func (cmr *CachedMapReduce[T, PT, U]) MapReduce(ctx context.Context, root *Node) (U, error) {
	var res U
	if root == nil {
		return res, errors.New("root is nil")
	}
	ce, err := cmr.mapReduceInternal(ctx, root)
	if err != nil {
		return res, err
	}
	return ce.value, nil
}

func (cmr *CachedMapReduce[T, PT, U]) mapReduceInternal(ctx context.Context, node *Node) (cacheEntry[U], error) {
	var res cacheEntry[U]

	Us := make([]U, 0)
	weight := 1
	for _, p := range node.Pointers {
		if p.cache != nil && p.dirty {
			return res, errors.New("cannot iterate over a dirty node")
		}
		if p.isShard() {
			if p.cache != nil && p.dirty {
				return res, errors.New("cannot iterate over a dirty node")
			}
			linkU, ok := cmr.cache.Get(p.Link)
			if !ok {
				chnd, err := p.loadChild(ctx, node.store, node.bitWidth, node.hash)
				if err != nil {
					return res, fmt.Errorf("loading child: %w", err)
				}

				linkU, err = cmr.mapReduceInternal(ctx, chnd)
				if err != nil {
					return res, fmt.Errorf("map reduce child: %w", err)
				}
				cmr.cache.Add(p.Link, linkU)
			}
			Us = append(Us, linkU.value)
			weight += linkU.weight
		} else {
			for _, v := range p.KVs {
				var pt = PT(new(T))
				err := pt.UnmarshalCBOR(bytes.NewReader(v.Value.Raw))
				if err != nil {
					return res, fmt.Errorf("failed to unmarshal value: %w", err)
				}
				u, err := cmr.mapper(string(v.Key), *pt)
				if err != nil {
					return res, fmt.Errorf("failed to map value: %w", err)
				}

				Us = append(Us, u)
			}
		}
	}

	resU, err := cmr.reducer(Us)
	if err != nil {
		return res, fmt.Errorf("failed to reduce self values: %w", err)
	}

	return cacheEntry[U]{
		value:  resU,
		weight: weight,
	}, nil
}
