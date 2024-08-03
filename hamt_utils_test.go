package hamt

import (
	"fmt"
	"io"

	cbg "github.com/whyrusleeping/cbor-gen"
)

// test utilities
// from https://github.com/polydawn/refmt/blob/3d65705ee9f12dc0dfcc0dc6cf9666e97b93f339/cbor/cborFixtures_test.go#L81-L93

func bcat(bss ...[]byte) []byte {
	l := 0
	for _, bs := range bss {
		l += len(bs)
	}
	rbs := make([]byte, 0, l)
	for _, bs := range bss {
		rbs = append(rbs, bs...)
	}
	return rbs
}

func b(b byte) []byte { return []byte{b} }

// A CBOR-marshalable byte array.
type CborByteArray []byte

func (c *CborByteArray) New() *CborByteArray {
	return new(CborByteArray)
}

func (c *CborByteArray) Equals(o *CborByteArray) bool {
	if c == nil && o == nil {
		return true
	}
	if c == nil || o == nil {
		return false
	}
	return string(*c) == string(*o)
}

func (c CborByteArray) MarshalCBOR(w io.Writer) error {
	if err := cbg.WriteMajorTypeHeader(w, cbg.MajByteString, uint64(len(c))); err != nil {
		return err
	}
	_, err := w.Write(c)
	return err
}

func (c *CborByteArray) UnmarshalCBOR(r io.Reader) error {
	maj, extra, err := cbg.CborReadHeader(r)
	if err != nil {
		return err
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	if uint64(cap(*c)) < extra {
		*c = make([]byte, extra)
	}
	if _, err := io.ReadFull(r, *c); err != nil {
		return err
	}
	return nil
}

func cborstr(s string) *CborByteArray {
	v := CborByteArray(s)
	vp := &v
	return vp
}

type CborInt int64

func (c *CborInt) New() *CborInt {
	return new(CborInt)
}

func (c *CborInt) Equals(o *CborInt) bool {
	if c == nil && o == nil {
		return true
	}
	if c == nil || o == nil {
		return false
	}
	return *c == *o
}

func (ci CborInt) MarshalCBOR(w io.Writer) error {
	v := int64(ci)
	if v >= 0 {
		if err := cbg.WriteMajorTypeHeader(w, cbg.MajUnsignedInt, uint64(v)); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteMajorTypeHeader(w, cbg.MajNegativeInt, uint64(-v)-1); err != nil {
			return err
		}
	}
	return nil
}

func (ci *CborInt) UnmarshalCBOR(r io.Reader) error {
	maj, extra, err := cbg.CborReadHeader(r)
	if err != nil {
		return err
	}
	var extraI int64
	switch maj {
	case cbg.MajUnsignedInt:
		extraI = int64(extra)
		if extraI < 0 {
			return fmt.Errorf("int64 positive overflow")
		}
	case cbg.MajNegativeInt:
		extraI = int64(extra)
		if extraI < 0 {
			return fmt.Errorf("int64 negative overflow")
		}
		extraI = -1 - extraI
	default:
		return fmt.Errorf("wrong type for int64 field: %d", maj)
	}

	*ci = CborInt(extraI)
	return nil
}
