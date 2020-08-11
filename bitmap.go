package hamt

import (
	"fmt"
	"math"
	"math/bits"
)

// Bitmap is a managed bitmap, primarily for the purpose of tracking the
// presence or absence of elements in an associated array. It can set and unset
// individual bits and perform limited popcount for a given index to calculate
// the position in the associated compacted array.
type Bitmap struct {
	Bytes []byte
}

// NewBitmap creates a new bitmap for a given bitWidth. The bitmap will hold
// 2^bitWidth bytes.
func NewBitmap(bitWidth int) *Bitmap {
	bc := (1 << uint(bitWidth)) / 8
	if bc == 0 {
		panic("bitWidth too small")
	}

	return NewBitmapFrom(make([]byte, bc))
}

// NewBitmapFrom creates a new Bitmap from an existing byte array. It is
// assumed that bytes is the correct length for the bitWidth of this Bitmap.
func NewBitmapFrom(bytes []byte) *Bitmap {
	if len(bytes) == 0 {
		panic("can't form Bitmap from zero bytes")
	}
	bm := Bitmap{Bytes: bytes}
	return &bm
}

// BitWidth calculates the bitWidth of this Bitmap by performing a
// log2(bits). The bitWidth is the minimum number of bits required to
// form indexes that address all of this Bitmap. e.g. a bitWidth of 5 can form
// indexes of 0 to 31, i.e. 4 bytes.
func (bm *Bitmap) BitWidth() int {
	return int(math.Log2(float64(len(bm.Bytes) * 8)))
}

func (bm *Bitmap) bindex(in int) int {
	// Return `in` to flip the byte addressing order to LE. For BE we address
	// from the last byte backward.
	bi := len(bm.Bytes) - 1 - in
	if bi > len(bm.Bytes) || bi < 0 {
		panic(fmt.Sprintf("invalid index for this Bitmap (index: %v, bytes: %v)", in, len(bm.Bytes)))
	}
	return bi
}

// IsSet indicates whether the bit at the provided position is set or not.
func (bm *Bitmap) IsSet(position int) bool {
	byt := bm.bindex(position / 8)
	offset := uint(position % 8)
	return (bm.Bytes[byt]>>offset)&1 == 1
}

// Set sets or unsets the bit at the given position according. If set is true,
// the bit will be set. If set is false, the bit will be unset.
func (bm *Bitmap) Set(position int, set bool) {
	has := bm.IsSet(position)
	byt := bm.bindex(position / 8)
	offset := uint(position % 8)

	if set && !has {
		bm.Bytes[byt] |= 1 << offset
	} else if !set && has {
		bm.Bytes[byt] ^= 1 << offset
	}
}

// Index performs a limited popcount up to the given position. This calculates
// the number of set bits up to the index of the bitmap. Useful for calculating
// the position of an element in an associated compacted array.
func (bm *Bitmap) Index(position int) int {
	t := 0
	eb := position / 8
	byt := 0
	for ; byt < eb; byt++ {
		// quick popcount for the full bytes
		t += bits.OnesCount(uint(bm.Bytes[bm.bindex(byt)]))
	}
	eb = eb * 8
	if position > eb {
		for i := byt * 8; i < position; i++ {
			// manual per-bit check for the remainder <8 bits
			if bm.IsSet(i) {
				t++
			}
		}
	}
	return t
}

// Copy creates a clone of the Bitmap, creating a new byte array with the same
// contents as the original.
func (bm *Bitmap) Copy() *Bitmap {
	ba := make([]byte, len(bm.Bytes))
	copy(ba, bm.Bytes)
	return NewBitmapFrom(ba)
}

// BitsSetCount counts how many bits are set in the bitmap.
func (bm *Bitmap) BitsSetCount() int {
	count := 0
	for _, b := range bm.Bytes {
		count += bits.OnesCount(uint(b))
	}
	return count
}
