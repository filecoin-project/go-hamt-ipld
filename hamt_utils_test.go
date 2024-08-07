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

func (c *CborByteArray) Equals(o *CborByteArray) bool {
	if c == nil && o == nil {
		return true
	}
	if c == nil || o == nil {
		return false
	}
	return string(*c) == string(*o)
}

func (c *CborByteArray) FromCBOR(r io.Reader) (*CborByteArray, error) {
	ci := CborByteArray{}
	if err := ci.UnmarshalCBOR(r); err != nil {
		return nil, err
	}
	return &ci, nil
}

func (c *CborByteArray) ToCBOR(w io.Writer) error {
	return c.MarshalCBOR(w)
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

func (c CborInt) Equals(o CborInt) bool {
	return c == o
}

func (t CborInt) FromCBOR(r io.Reader) (CborInt, error) {
	ci := new(CborInt)
	if err := ci.UnmarshalCBOR(r); err != nil {
		return CborInt(0), err
	}
	return *ci, nil
}

func (t CborInt) ToCBOR(w io.Writer) error {
	return t.MarshalCBOR(w)
}

func (t *CborInt) MarshalCBOR(w io.Writer) error {
	cw := cbg.NewCborWriter(w)

	// (*t) (testing.CborInt) (int64)
	if (*t) >= 0 {
		if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64((*t))); err != nil {
			return err
		}
	} else {
		if err := cw.WriteMajorTypeHeader(cbg.MajNegativeInt, uint64(-(*t)-1)); err != nil {
			return err
		}
	}

	return nil
}

func (t *CborInt) UnmarshalCBOR(r io.Reader) (err error) {
	*t = CborInt(0)

	cr := cbg.NewCborReader(r)
	var maj byte
	var extra uint64
	_ = maj
	_ = extra

	// (*t) (testing.CborInt) (int64)
	{
		maj, extra, err := cr.ReadHeader()
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

		(*t) = CborInt(extraI)
	}

	return nil
}
