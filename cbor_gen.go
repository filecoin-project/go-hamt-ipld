// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

package hamt

import (
	"fmt"
	"io"
	"math"
	"sort"

	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf
var _ = cid.Undef
var _ = math.E
var _ = sort.Sort

var lengthBufNode = []byte{130}

func (t *Node) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write(lengthBufNode); err != nil {
		return err
	}

	// t.Bitfield (hamt.Bitfield) (struct)
	if err := t.Bitfield.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.Pointers ([]*hamt.Pointer) (slice)
	if len(t.Pointers) > 8192 {
		return xerrors.Errorf("Slice value in field t.Pointers was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajArray, uint64(len(t.Pointers))); err != nil {
		return err
	}
	for _, v := range t.Pointers {
		if err := v.MarshalCBOR(cw); err != nil {
			return err
		}

	}
	return nil
}

func (t *Node) UnmarshalCBOR(r io.Reader) (err error) {
	*t = Node{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Bitfield (hamt.Bitfield) (struct)

	{

		if err := t.Bitfield.UnmarshalCBOR(cr); err != nil {
			return xerrors.Errorf("unmarshaling t.Bitfield: %w", err)
		}

	}
	// t.Pointers ([]*hamt.Pointer) (slice)

	maj, extra, err = cr.ReadHeader()
	if err != nil {
		return err
	}

	if extra > 8192 {
		return fmt.Errorf("t.Pointers: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}

	if extra > 0 {
		t.Pointers = make([]*Pointer, extra)
	}

	for i := 0; i < int(extra); i++ {
		{
			var maj byte
			var extra uint64
			var err error
			_ = maj
			_ = extra
			_ = err

			{

				b, err := cr.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := cr.UnreadByte(); err != nil {
						return err
					}
					t.Pointers[i] = new(Pointer)
					if err := t.Pointers[i].UnmarshalCBOR(cr); err != nil {
						return xerrors.Errorf("unmarshaling t.Pointers[i] pointer: %w", err)
					}
				}

			}

		}
	}
	return nil
}

var lengthBufKV = []byte{130}

func (t *KV) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write(lengthBufKV); err != nil {
		return err
	}

	// t.Key ([]uint8) (slice)
	if len(t.Key) > 2097152 {
		return xerrors.Errorf("Byte array in field t.Key was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajByteString, uint64(len(t.Key))); err != nil {
		return err
	}

	if _, err := cw.Write(t.Key); err != nil {
		return err
	}

	// t.Value (typegen.Deferred) (struct)
	if err := t.Value.MarshalCBOR(cw); err != nil {
		return err
	}
	return nil
}

func (t *KV) UnmarshalCBOR(r io.Reader) (err error) {
	*t = KV{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Key ([]uint8) (slice)

	maj, extra, err = cr.ReadHeader()
	if err != nil {
		return err
	}

	if extra > 2097152 {
		return fmt.Errorf("t.Key: byte array too large (%d)", extra)
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}

	if extra > 0 {
		t.Key = make([]uint8, extra)
	}

	if _, err := io.ReadFull(cr, t.Key); err != nil {
		return err
	}

	// t.Value (typegen.Deferred) (struct)

	{

		t.Value = new(cbg.Deferred)

		if err := t.Value.UnmarshalCBOR(cr); err != nil {
			return xerrors.Errorf("failed to read deferred field: %w", err)
		}
	}
	return nil
}
