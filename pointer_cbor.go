package hamt

import (
	"fmt"
	"io"

	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// implemented as a kinded union - a "Pointer" is either a Link (child node) or
// an Array (bucket)

func (t *Pointer) MarshalCBOR(w io.Writer) error {
	if t.Link != cid.Undef && len(t.KVs) > 0 {
		return fmt.Errorf("hamt Pointer cannot have both a link and KVs")
	}

	scratch := make([]byte, 9)

	if t.Link != cid.Undef {
		if err := cbg.WriteCidBuf(scratch, w, t.Link); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajArray, uint64(len(t.KVs))); err != nil {
			return err
		}

		for _, kv := range t.KVs {
			if err := kv.MarshalCBOR(w); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *Pointer) UnmarshalCBOR(br io.Reader) error {
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}

	if maj == cbg.MajTag {
		if extra != 42 {
			return fmt.Errorf("expected tag 42 for child node link")
		}

		ba, err := cbg.ReadByteArray(br, 512)
		if err != nil {
			return err
		}

		c, err := bufToCid(ba)
		if err != nil {
			return err
		}

		t.Link = c
		return nil
	} else if maj == cbg.MajArray {
		length := extra

		if length > 32 {
			return fmt.Errorf("KV array in CBOR input for pointer was too long")
		}

		t.KVs = make([]*KV, length)
		for i := 0; i < int(length); i++ {
			var kv KV
			if err := kv.UnmarshalCBOR(br); err != nil {
				return err
			}

			t.KVs[i] = &kv
		}

		return nil
	} else {
		return fmt.Errorf("expected CBOR child node link or array")
	}
}

// from https://github.com/whyrusleeping/cbor-gen/blob/211df3b9e24c6e0d0c338b440e6ab4ab298505b2/utils.go#L530
func bufToCid(buf []byte) (cid.Cid, error) {
	if len(buf) == 0 {
		return cid.Undef, fmt.Errorf("undefined CID")
	}

	if len(buf) < 2 {
		return cid.Undef, fmt.Errorf("DAG-CBOR serialized CIDs must have at least two bytes")
	}

	if buf[0] != 0 {
		return cid.Undef, fmt.Errorf("DAG-CBOR serialized CIDs must have binary multibase")
	}

	return cid.Cast(buf[1:])
}
