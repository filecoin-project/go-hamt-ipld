package hamt

import (
	"bytes"
	"testing"
)

// many cases taken from https://github.com/rvagg/iamap/blob/fad95295b013c8b4f0faac6dd5d9be175f6e606c/test/bit-utils-test.js
// but rev() is used to reverse the data in most instances

// reverse for BE format
func rev(in []byte) []byte {
	out := make([]byte, len(in))
	for i := 0; i < len(in); i++ {
		out[len(in)-1-i] = in[i]
	}
	return out
}

// 8-char binary string to byte, no binary literals in old Go
func bb(s string) byte {
	var r byte
	for i, c := range s {
		if c == '1' {
			r |= 1 << uint(7-i)
		}
	}
	return r
}

func TestBitmapHas(t *testing.T) {
	type tcase struct {
		bytes []byte
		pos   int
		set   bool
	}
	cases := []tcase{
		{b(0x0), 0, false},
		{b(0x1), 0, true},
		{b(bb("00101010")), 2, false},
		{b(bb("00101010")), 3, true},
		{b(bb("00101010")), 4, false},
		{b(bb("00101010")), 5, true},
		{b(bb("00100000")), 5, true},
		{[]byte{0x0, bb("00100000")}, 8 + 5, true},
		{[]byte{0x0, 0x0, bb("00100000")}, 8*2 + 5, true},
		{[]byte{0x0, 0x0, 0x0, bb("00100000")}, 8*3 + 5, true},
		{[]byte{0x0, 0x0, 0x0, 0x0, bb("00100000")}, 8*4 + 5, true},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, bb("00100000")}, 8*5 + 5, true},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, bb("00100000")}, 8*4 + 5, false},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, bb("00100000")}, 8*3 + 5, false},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, bb("00100000")}, 8*2 + 5, false},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, bb("00100000")}, 8 + 5, false},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, bb("00100000")}, 5, false},
	}

	for _, c := range cases {
		bm := NewBitmapFrom(rev(c.bytes))
		if bm.IsSet(c.pos) != c.set {
			t.Fatalf("bitmap %v IsSet(%v) should be %v", c.bytes, c.pos, c.set)
		}
	}
}

func TestBitmapBitWidth(t *testing.T) {
	for i := 3; i <= 16; i++ {
		if NewBitmap(i).BitWidth() != i {
			t.Fatal("incorrect bitWidth calculation")
		}
		if NewBitmapFrom(make([]byte, (1<<uint(i))/8)).BitWidth() != i {
			t.Fatal("incorrect bitWidth calculation")
		}
	}
}

func TestBitmapIndex(t *testing.T) {
	type tcase struct {
		bytes    []byte
		pos      int
		expected int
	}
	cases := []tcase{
		{b(bb("00111111")), 0, 0},
		{b(bb("00111111")), 1, 1},
		{b(bb("00111111")), 2, 2},
		{b(bb("00111111")), 4, 4},
		{b(bb("00111100")), 2, 0},
		{b(bb("00111101")), 4, 3},
		{b(bb("00111001")), 4, 2},
		{b(bb("00111000")), 4, 1},
		{b(bb("00110000")), 4, 0},
		{b(bb("00000000")), 0, 0},
		{b(bb("00000000")), 1, 0},
		{b(bb("00000000")), 2, 0},
		{b(bb("00000000")), 3, 0},
		{[]byte{0x0, 0x0, 0x0}, 20, 0},
		{[]byte{0xff, 0xff, 0xff}, 5, 5},
		{[]byte{0xff, 0xff, 0xff}, 7, 7},
		{[]byte{0xff, 0xff, 0xff}, 8, 8},
		{[]byte{0xff, 0xff, 0xff}, 10, 10},
		{[]byte{0xff, 0xff, 0xff}, 20, 20},
	}

	for _, c := range cases {
		bm := NewBitmapFrom(rev(c.bytes))
		if bm.Index(c.pos) != c.expected {
			t.Fatalf("bitmap %v Index(%v) should be %v", c.bytes, c.pos, c.expected)
		}
	}
}

func TestBitmap_32bitFixed(t *testing.T) {
	// a 32-byte bitmap and a list of all the bits that are set
	byts := []byte{
		bb("00100101"), bb("10000000"), bb("00000000"), bb("01000000"),
		bb("00000000"), bb("01000000"), bb("00000000"), bb("01000000"),
		bb("00000000"), bb("00100000"), bb("00000000"), bb("01000000"),
		bb("00000000"), bb("00010000"), bb("00000000"), bb("01000000"),
		bb("00000000"), bb("00001000"), bb("00000000"), bb("01000000"),
		bb("00000000"), bb("00000100"), bb("00000000"), bb("01000000"),
		bb("00000000"), bb("00000010"), bb("00000000"), bb("01000000"),
		bb("00000000"), bb("00000001"), bb("00000000"), bb("01000000"),
	}
	bm := NewBitmapFrom(rev(byts))
	set := []int{
		0, 2, 5, 8 + 7, 8*3 + 6,
		8*5 + 6, 8*7 + 6,
		8*9 + 5, 8*11 + 6,
		8*13 + 4, 8*15 + 6,
		8*17 + 3, 8*19 + 6,
		8*21 + 2, 8*23 + 6,
		8*25 + 1, 8*27 + 6,
		8 * 29, 8*31 + 6}

	c := 0
	for i := 0; i < 256; i++ {
		if c < len(set) && i == set[c] {
			if !bm.IsSet(i) {
				t.Fatalf("IsSet(%v) should be true", i)
			}
			// the index c of `set` also gives us the translation of Index(i)
			if bm.Index(i) != c {
				t.Fatalf("Index(%v) should be %v", i, c)
			}
			c++
		} else {
			if bm.IsSet(i) {
				t.Fatalf("IsSet(%v) should be false", i)
			}
		}
	}
}

func TestBitmapSetBytes(t *testing.T) {
	newSet := func(bitWidth int, ba []byte, index int, set bool) []byte {
		var bm *Bitmap
		if ba != nil {
			bm = NewBitmapFrom(ba)
		} else {
			bm = NewBitmap(bitWidth)
		}
		bm.Set(index, set)
		return bm.Bytes
	}

	if !bytes.Equal(newSet(3, nil, 0, true), rev([]byte{bb("00000001")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(3, nil, 1, true), rev([]byte{bb("00000010")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(3, nil, 7, true), rev([]byte{bb("10000000")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11111111")}), 0, true), rev([]byte{bb("11111111")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11111111")}), 7, true), rev([]byte{bb("11111111")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("01010101")}), 1, true), rev([]byte{bb("01010111")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("01010101")}), 7, true), rev([]byte{bb("11010101")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11111111")}), 0, false), rev([]byte{bb("11111110")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11111111")}), 1, false), rev([]byte{bb("11111101")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11111111")}), 7, false), rev([]byte{bb("01111111")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0, bb("11111111")}), 8+0, true), rev([]byte{0, bb("11111111")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0, bb("11111111")}), 8+7, true), rev([]byte{0, bb("11111111")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0, bb("01010101")}), 8+1, true), rev([]byte{0, bb("01010111")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0, bb("01010101")}), 8+7, true), rev([]byte{0, bb("11010101")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0, bb("11111111")}), 8+0, false), rev([]byte{0, bb("11111110")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0, bb("11111111")}), 8+1, false), rev([]byte{0, bb("11111101")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0, bb("11111111")}), 8+7, false), rev([]byte{0, bb("01111111")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0}), 0, false), rev([]byte{bb("00000000")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0}), 7, false), rev([]byte{bb("00000000")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("01010101")}), 0, false), rev([]byte{bb("01010100")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("01010101")}), 6, false), rev([]byte{bb("00010101")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")}), 0, false), rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")}), 0, true), rev([]byte{bb("11000011"), bb("11010010"), bb("01001010"), bb("00000001")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")}), 12, false), rev([]byte{bb("11000010"), bb("11000010"), bb("01001010"), bb("00000001")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")}), 12, true), rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")}), 24, false), rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000000")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")}), 24, true), rev([]byte{bb("11000010"), bb("11010010"), bb("01001010"), bb("00000001")})) {
		t.Fatal("Failed bytes comparison")
	}
	if !bytes.Equal(newSet(0, rev([]byte{0, 0, 0, 0}), 31, true), rev([]byte{0, 0, 0, bb("10000000")})) {
		t.Fatal("Failed bytes comparison")
	}
}
