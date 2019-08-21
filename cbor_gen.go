package hamt

import (
	"fmt"
	"io"
	"math/big"

	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

// NOTE: This is a generated file, but it has been modified to encode the
// bitfield big.Int as a byte array. The bitfield is only a big.Int because
// thats a convenient type for the operations we need to perform on it, but it
// is fundamentally an array of bytes (bits)

var _ = xerrors.Errorf

func (t *Node) MarshalCBOR(w io.Writer) error {
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.t.Bitfield (big.Int)
	{
		var b []byte
		if t.Bitfield != nil {
			b = t.Bitfield.Bytes()
		}

		if err := cbg.CborWriteHeader(w, cbg.MajByteString, uint64(len(b))); err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
	}

	// t.t.Pointers ([]*hamt.Pointer)
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajArray, uint64(len(t.Pointers)))); err != nil {
		return err
	}
	for _, v := range t.Pointers {
		if err := v.MarshalCBOR(w); err != nil {
			return err
		}
	}
	return nil
}

func (t *Node) UnmarshalCBOR(br io.Reader) error {

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.t.Bitfield (big.Int)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}

	if maj != cbg.MajByteString {
		return fmt.Errorf("big ints should be tagged cbor byte strings")
	}

	if extra > 256 {
		return fmt.Errorf("cbor bignum was too large")
	}

	if extra > 0 {
		buf := make([]byte, extra)
		if _, err := io.ReadFull(br, buf); err != nil {
			return err
		}
		t.Bitfield = big.NewInt(0).SetBytes(buf)
	}
	// t.t.Pointers ([]*hamt.Pointer)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if extra > 8192 {
		return fmt.Errorf("array too large")
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}
	if extra > 0 {
		t.Pointers = make([]*Pointer, extra)
	}
	for i := 0; i < int(extra); i++ {
		var v Pointer
		if err := v.UnmarshalCBOR(br); err != nil {
			return err
		}

		t.Pointers[i] = &v
	}

	return nil
}

func (t *KV) MarshalCBOR(w io.Writer) error {
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.t.Key (string)
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajTextString, uint64(len(t.Key)))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(t.Key)); err != nil {
		return err
	}

	// t.t.Value (typegen.Deferred)

	if err := t.Value.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *KV) UnmarshalCBOR(br io.Reader) error {

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.t.Key (string)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}

	if maj != cbg.MajTextString {
		return fmt.Errorf("expected cbor type 'text string' in input")
	}

	if extra > 256*1024 {
		return fmt.Errorf("string in cbor input too long")
	}

	{
		buf := make([]byte, extra)
		if _, err := io.ReadFull(br, buf); err != nil {
			return err
		}

		t.Key = string(buf)
	}
	// t.t.Value (typegen.Deferred)

	t.Value = new(cbg.Deferred)

	if err := t.Value.UnmarshalCBOR(br); err != nil {
		return err
	}
	return nil
}
