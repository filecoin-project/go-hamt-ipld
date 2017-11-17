package hamt

import (
	"crypto/md5"
	"fmt"
	"math/big"
)

type Node struct {
	Bitfield *big.Int
	Pointers []*Pointer
}

func NewNode() *Node {
	return &Node{
		Bitfield: big.NewInt(0),
	}
}

type Pointer struct {
	Prefix *byte
	Key    string
	Obj    interface{}
}

func hash(k string) []byte {
	s := md5.Sum([]byte(k))
	return s[:]
}

func (n *Node) Find(k string) (string, error) {
	var out string
	err := n.getValue(hash(k), 0, k, func(p *Pointer) error {
		out = p.Obj.(string)
		return nil
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func (n *Node) Delete(k string) error {
	return n.modifyValue(hash(k), 0, k, nil)
}

var ErrNotFound = fmt.Errorf("not found")

func (n *Node) getValue(hv []byte, depth int, k string, cb func(*Pointer) error) error {
	idx := hv[depth]
	if n.Bitfield.Bit(int(idx)) == 0 {
		return ErrNotFound
	}

	cindex := byte(n.indexForBitPos(int(idx)))

	child := n.getChild(cindex)

	switch child := child.(type) {
	case *Pointer:
		if child.Prefix != nil {
			return child.Obj.(*Node).getValue(hv, depth+1, k, cb)
		}

		if child.Key == k {
			return cb(child)
		}

		return ErrNotFound
	case []*Pointer:
		panic("NYI")
	default:
		panic("invariant invalidated")
	}
}

func (n *Node) Set(k string, v string) error {
	return n.modifyValue(hash(k), 0, k, v)
}

func (n *Node) modifyValue(hv []byte, depth int, k string, v interface{}) error {
	idx := int(hv[depth])

	if n.Bitfield.Bit(idx) != 1 {
		return n.insertChild(idx, k, v)
	}

	cindex := byte(n.indexForBitPos(idx))

	switch child := n.getChild(cindex).(type) {
	case *Pointer:
		if child.Prefix != nil {
			chnd := child.Obj.(*Node)
			if err := chnd.modifyValue(hv, depth+1, k, v); err != nil {
				return err
			}

			// CHAMP optimization, ensure trees look correct after deletions
			if v == nil {
				switch len(chnd.Pointers) {
				case 0:
					return fmt.Errorf("incorrectly formed HAMT")
				case 1:
					n.setChild(cindex, chnd.Pointers[0])
					return nil
				}
			}
			return nil
		}

		if child.Key == k {
			if v == nil {
				n.Bitfield.SetBit(n.Bitfield, idx, 0)
				return n.rmChild(cindex)
			}
			child.Obj = v
			return nil
		} else {
			if v == nil {
				return ErrNotFound
			}
			// TODO: 'pair' optimization

			splnode := NewNode()

			splnode.modifyValue(hv, depth+1, k, v)
			ohv := hash(child.Key)
			splnode.modifyValue(ohv, depth+1, child.Key, child.Obj.(string))

			n.setChild(cindex, &Pointer{Prefix: &cindex, Obj: splnode})
			return nil
		}
	case []*Pointer:
		panic("not yet handled")
	default:
		panic("no")
	}
}

func (n *Node) insertChild(idx int, k string, v interface{}) error {
	if v == nil {
		return ErrNotFound
	}

	i := n.indexForBitPos(idx)
	n.Bitfield.SetBit(n.Bitfield, idx, 1)

	p := &Pointer{Key: k, Obj: v}

	n.Pointers = append(n.Pointers[:i], append([]*Pointer{p}, n.Pointers[i:]...)...)
	return nil
}

func (n *Node) setChild(i byte, p *Pointer) {
	n.Pointers[i] = p
}

func (n *Node) rmChild(i byte) error {
	copy(n.Pointers[i:], n.Pointers[i+1:])
	n.Pointers = n.Pointers[:len(n.Pointers)-1]

	return nil
}

func (n *Node) getChild(i byte) interface{} {
	if int(i) >= len(n.Pointers) || i < 0 {
		return nil
	}

	return n.Pointers[i]
}
