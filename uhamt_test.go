package hamt

import (
	"math/big"
	"math/bits"
	"math/rand"
	"testing"
)

func TestIndexForBitRandom(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewSource(int64(42)))

	count := 100000
	slot := make([]byte, 32)
	for i := 0; i < count; i++ {
		_, err := r.Read(slot)
		if err != nil {
			t.Fatal("couldn't create random bitfield")
		}
		bi := big.NewInt(0).SetBytes(slot)
		for k := 0; k < 256; k++ {
			if indexForBitPos(k, bi) != indexForBitPosOriginal(k, bi) {
				t.Fatalf("indexForBit doesn't match with original")
			}
		}
	}
}

func TestIndexForBitLinear(t *testing.T) {
	t.Parallel()
	var i int64
	for i = 0; i < 1<<16-1; i++ {
		bi := big.NewInt(i)
		for k := 0; k < 16; k++ {
			if indexForBitPos(k, bi) != indexForBitPosOriginal(k, bi) {
				t.Fatalf("indexForBit doesn't match with original")
			}
		}
	}
}

// Original implementation of indexForBit, before #39.
func indexForBitPosOriginal(bp int, bitfield *big.Int) int {
	mask := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(bp)), nil), big.NewInt(1))
	mask.And(mask, bitfield)

	return popCount(mask)
}

func popCount(i *big.Int) int {
	var n int
	for _, v := range i.Bits() {
		n += bits.OnesCount64(uint64(v))
	}
	return n
}
