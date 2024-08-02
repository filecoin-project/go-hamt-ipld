package hamt

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleEquals(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	prev, err := NewNode[*CborByteArray](prevBs)
	assert.NoError(t, err)

	cur, err := NewNode[*CborByteArray](curBs)
	assert.NoError(t, err)

	_ = diffAndAssertLength(ctx, t, prevBs, curBs, prev, cur, 0)

	assertSet(t, prev, 2, "foo")
	assertSet(t, cur, 2, "foo")

	_ = diffAndAssertLength(ctx, t, prevBs, curBs, prev, cur, 0)
}

func TestSimpleAdd(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewNode[*CborByteArray](prevBs)
	assert.NoError(t, err)

	b, err := NewNode[*CborByteArray](curBs)
	assert.NoError(t, err)

	assertSet(t, a, 2, "foo")
	assertGet(ctx, t, a, 2, "foo")

	assertSet(t, b, 2, "foo")
	assertSet(t, b, 5, "bar")

	assertGet(ctx, t, b, 2, "foo")
	assertGet(ctx, t, b, 5, "bar")

	cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1)

	ec := expectedChange{
		Type:   Add,
		Key:    "5",
		Before: "",
		After:  "bar",
	}

	ec.assertExpectation(t, cs[0])
}

func TestSimpleRemove(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewNode[*CborByteArray](prevBs)
	assert.NoError(t, err)

	b, err := NewNode[*CborByteArray](curBs)
	assert.NoError(t, err)

	assertSet(t, a, 2, "foo")
	assertSet(t, a, 5, "bar")

	assertGet(ctx, t, a, 2, "foo")
	assertGet(ctx, t, a, 5, "bar")

	assertSet(t, b, 2, "foo")
	assertGet(ctx, t, b, 2, "foo")

	cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1)

	ec := expectedChange{
		Type:   Remove,
		Key:    "5",
		Before: "bar",
		After:  "",
	}

	ec.assertExpectation(t, cs[0])
}

func TestSimpleModify(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewNode[*CborByteArray](prevBs)
	assert.NoError(t, err)

	b, err := NewNode[*CborByteArray](curBs)
	assert.NoError(t, err)

	assertSet(t, a, 2, "foo")
	assertSet(t, b, 2, "bar")

	cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1)

	ec := expectedChange{
		Type:   Modify,
		Key:    "2",
		Before: "foo",
		After:  "bar",
	}

	ec.assertExpectation(t, cs[0])
}

func TestLargeModify(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewNode[*CborByteArray](prevBs)
	assert.NoError(t, err)

	b, err := NewNode[*CborByteArray](curBs)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		assertSet(t, a, i, "foo"+strconv.Itoa(i))
	}

	ecs := make([]expectedChange, 0)

	// modify every other element, 50 modifies + 50 removes
	for i := 0; i < 100; i += 2 {
		assertSet(t, b, i, "bar"+strconv.Itoa(i))

		ecs = append(ecs, expectedChange{
			Type:   Modify,
			Key:    strconv.Itoa(i),
			Before: "foo" + strconv.Itoa(i),
			After:  "bar" + strconv.Itoa(i),
		})

		ecs = append(ecs, expectedChange{
			Type:   Remove,
			Key:    strconv.Itoa(i + 1),
			Before: "foo" + strconv.Itoa(i+1),
			After:  "",
		})
	}

	cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 100)

	sort.Slice(cs, func(i, j int) bool {
		ik, err := strconv.Atoi(cs[i].Key)
		if err != nil {
			t.Fatal(err)
		}
		jk, err := strconv.Atoi(cs[j].Key)
		if err != nil {
			t.Fatal(err)
		}
		return ik < jk
	})

	for i := range cs {
		ecs[i].assertExpectation(t, cs[i])
	}
}

func TestLargeAdditions(t *testing.T) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewNode[*CborByteArray](prevBs)
	assert.NoError(t, err)

	b, err := NewNode[*CborByteArray](curBs)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		assertSet(t, a, i, "foo"+strconv.Itoa(i))
		assertSet(t, b, i, "foo"+strconv.Itoa(i))
	}

	ecs := make([]expectedChange, 0)

	// new additions, 500 additions
	for i := 2000; i < 2500; i++ {
		assertSet(t, b, i, "bar"+strconv.Itoa(i))

		ecs = append(ecs, expectedChange{
			Type:   Add,
			Key:    strconv.Itoa(i),
			Before: "",
			After:  "bar" + strconv.Itoa(i),
		})
	}

	cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 500)

	sort.Slice(cs, func(i, j int) bool {
		ik, err := strconv.Atoi(cs[i].Key)
		if err != nil {
			t.Fatal(err)
		}
		jk, err := strconv.Atoi(cs[j].Key)
		if err != nil {
			t.Fatal(err)
		}
		return ik < jk
	})

	for i := range cs {
		ecs[i].assertExpectation(t, cs[i])
	}
}

func bigDiff(t *testing.T, scale int) {
	prevBs := cbor.NewCborStore(newMockBlocks())
	curBs := cbor.NewCborStore(newMockBlocks())
	ctx := context.Background()

	a, err := NewNode[*CborByteArray](prevBs)
	assert.NoError(t, err)

	b, err := NewNode[*CborByteArray](curBs)
	assert.NoError(t, err)

	for i := 0; i < 100*scale; i++ {
		assertSet(t, a, i, "foo"+strconv.Itoa(i))
	}

	ecs := make([]expectedChange, 0)

	// modify every other element, 50*scale modifies + 50*scale removes
	for i := 0; i < 100*scale; i += 2 {
		assertSet(t, b, i, "bar"+strconv.Itoa(i))

		ecs = append(ecs, expectedChange{
			Type:   Modify,
			Key:    strconv.Itoa(i),
			Before: "foo" + strconv.Itoa(i),
			After:  "bar" + strconv.Itoa(i),
		})

		ecs = append(ecs, expectedChange{
			Type:   Remove,
			Key:    strconv.Itoa(i + 1),
			Before: "foo" + strconv.Itoa(i+1),
			After:  "",
		})
	}

	// modify every element between 1000*scale and 1500*scale, 500*scale modifies
	for i := 1000 * scale; i < 1500*scale; i++ {
		assertSet(t, a, i, "foo"+strconv.Itoa(i))
		assertSet(t, b, i, "bar"+strconv.Itoa(i))

		ecs = append(ecs, expectedChange{
			Type:   Modify,
			Key:    strconv.Itoa(i),
			Before: "foo" + strconv.Itoa(i),
			After:  "bar" + strconv.Itoa(i),
		})
	}

	// new additions, 500*scale additions
	for i := 2000 * scale; i < 2500*scale; i++ {
		assertSet(t, b, i, "bar"+strconv.Itoa(i))

		ecs = append(ecs, expectedChange{
			Type:   Add,
			Key:    strconv.Itoa(i),
			Before: "",
			After:  "bar" + strconv.Itoa(i),
		})
	}

	// (10000-10249)*scale is removed, 250*scale removals
	for i := 10000 * scale; i < 10250*scale; i++ {
		assertSet(t, a, i, "foo"+strconv.Itoa(i))

		ecs = append(ecs, expectedChange{
			Type:   Remove,
			Key:    strconv.Itoa(i),
			Before: "foo" + strconv.Itoa(i),
			After:  "",
		})
	}

	// (10250-10500)*scale is modified, 250*scale modifies
	for i := 10250 * scale; i < 10500*scale; i++ {
		assertSet(t, a, i, "foo"+strconv.Itoa(i))
		assertSet(t, b, i, "bar"+strconv.Itoa(i))

		ecs = append(ecs, expectedChange{
			Type:   Modify,
			Key:    strconv.Itoa(i),
			Before: "foo" + strconv.Itoa(i),
			After:  "bar" + strconv.Itoa(i),
		})
	}

	cs := diffAndAssertLength(ctx, t, prevBs, curBs, a, b, 1600*scale)

	sort.Slice(cs, func(i, j int) bool {
		ik, err := strconv.Atoi(cs[i].Key)
		if err != nil {
			t.Fatal(err)
		}
		jk, err := strconv.Atoi(cs[j].Key)
		if err != nil {
			t.Fatal(err)
		}
		return ik < jk
	})

	t.Logf("Scale: %d, Change Size: %d", scale, len(cs))
	for i := range cs {
		ecs[i].assertExpectation(t, cs[i])
	}
}

func TestBigDiff(t *testing.T) {
	scales := []int{1, 2, 4, 8, 16, 32, 64}
	for _, scale := range scales {
		t.Run(fmt.Sprintf("BigDIff Scale %d", scale), func(t *testing.T) {
			bigDiff(t, scale)
		})
	}
}

func diffAndAssertLength(ctx context.Context, t *testing.T, prevBs, curBs cbor.IpldStore, a, b *Node[*CborByteArray], expectedLength int) []*Change[*CborByteArray] {
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	cs, err := diffNode(ctx, a, b, 0)
	if err != nil {
		t.Fatalf("unexpected error from diff: %v", err)
	}

	if len(cs) != expectedLength {
		t.Fatalf("got %d changes, wanted %d", len(cs), expectedLength)
	}

	cs, err = doParallelDiffNode(ctx, a, b, 4096)
	if err != nil {
		t.Fatalf("unexpected error from parallel diff: %v", err)
	}

	if len(cs) != expectedLength {
		t.Fatalf("got %d changes, wanted %d", len(cs), expectedLength)
	}

	return cs
}

func assertSet(t *testing.T, r *Node[*CborByteArray], i int, val string) {
	ctx := context.Background()

	t.Helper()
	if err := r.Set(ctx, strconv.Itoa(i), cborstr(val)); err != nil {
		t.Fatal(err)
	}
}

func assertGet(ctx context.Context, t testing.TB, r *Node[*CborByteArray], i int, val string) {
	t.Helper()
	found, _, err := r.Find(ctx, strconv.Itoa(i))
	require.NoError(t, err)
	require.True(t, found)

	found, out, err := r.Find(ctx, strconv.Itoa(i))
	require.NoError(t, err)
	require.True(t, found)

	if !out.Equal(*cborstr(val)) {
		t.Fatal("value we got out didnt match expectation")
	}
}

type expectedChange struct {
	Type   ChangeType
	Key    string
	Before string
	After  string
}

func (ec expectedChange) assertExpectation(t *testing.T, change *Change[*CborByteArray]) {
	assert.Equal(t, ec.Type, change.Type)
	assert.Equal(t, ec.Key, change.Key)

	switch ec.Type {
	case Add:
		assert.Nilf(t, change.Before, "before val should be nil for Add")
		assert.NotNilf(t, change.After, "after val shouldn't be nil for Add")
		assert.Equal(t, cborstr(ec.After), change.After)
	case Remove:
		assert.NotNilf(t, change.Before, "before val shouldn't be nil for Remove")
		assert.Nilf(t, change.After, "after val should be nil for Remove")
		assert.Equal(t, cborstr(ec.Before), change.Before)
	case Modify:
		assert.NotNilf(t, change.Before, "before val shouldn't be nil for Modify")
		assert.NotNilf(t, change.After, "after val shouldn't be nil for Modify")
		assert.Equal(t, cborstr(ec.Before), change.Before)
		assert.Equal(t, cborstr(ec.After), change.After)
	}
}
