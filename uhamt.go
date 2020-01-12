package hamt

import (
	"math/big"
	"math/bits"
)

// indexForBitPos returns the index within the collapsed array corresponding to
// the given bit in the bitset.  The collapsed array contains only one entry
// per bit set in the bitfield, and this function is used to map the indices.
func (n *Node) indexForBitPosOld(bp int) int {
	// TODO: an optimization could reuse the same 'mask' here and change the size
	//       as needed. This isnt yet done as the bitset package doesnt make it easy
	//       to do.

	// make a bitmask (all bits set) 'bp' bits long
	mask := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(bp)), nil), big.NewInt(1))
	mask.And(mask, n.Bitfield)

	return popCount(mask)
}

func popCount(i *big.Int) int {
	var n int
	for _, v := range i.Bits() {
		n += bits.OnesCount64(uint64(v))
	}
	return n
}

func (n *Node) indexForBitPos(bp int) int {
	var x uint
	var count, i int
	w := n.Bitfield.Bits()
	for x = uint(bp); x > bits.UintSize && i < len(w); x -= bits.UintSize {
		count += bits.OnesCount64(uint64(w[i]))
		i++
	}
	if i == len(w) {
		return count
	}
	return count + bits.OnesCount64(uint64(w[i])&((1<<x)-1))
}
