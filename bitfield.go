package hamt

import (
	"io"
	"math/big"

	cbg "github.com/whyrusleeping/cbor-gen"
)

type Bitfield struct {
	big.Int
}

func (b *Bitfield) MarshalCBOR(w io.Writer) error {
	return cbg.WriteByteArray(w, b.Bytes())
}

func (b *Bitfield) UnmarshalCBOR(r io.Reader) error {
	bytes, err := cbg.ReadByteArray(r, cbg.ByteArrayMaxLen)
	if err != nil {
		return err
	}
	b.SetBytes(bytes)

	return nil
}
