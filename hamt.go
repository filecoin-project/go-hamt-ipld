package hamt

import (
	"crypto/md5"
	"fmt"
	"math/big"
)

var ErrNotFound = fmt.Errorf("not found")

type Node struct {
	Bitfield *big.Int
	Pointers []*Pointer
}

func NewNode() *Node {
	return &Node{Bitfield: big.NewInt(0)}
}

type Pointer struct {
	Key *string
	Obj interface{}
}

func hash(k string) []byte {
	s := md5.Sum([]byte(k))
	return s[:]
}

func (n *Node) Find(k string) (string, bool) {
	return n.getValue(hash(k), 0, k)
}

func (n *Node) getValue(hv []byte, depth int, k string) (string, bool) {
	idx := hv[depth]
	if n.Bitfield.Bit(int(idx)) == 0 {
		return "", false
	}

	cindex := byte(n.indexForBitPos(int(idx)))

	child := n.Pointers[cindex]
	if child.Key == nil {
		return child.Obj.(*Node).getValue(hv, depth+1, k)
	}

	if *child.Key == k {
		return child.Obj.(string), true
	}

	return "", false
}

func (n *Node) Set(k string, v string) {
	n.modifyValue(hash(k), 0, k, v)
}

func (n *Node) modifyValue(hv []byte, depth int, k, v string) {
	idx := int(hv[depth])

	if n.Bitfield.Bit(idx) != 1 {
		n.insertChild(idx, k, v)
		return
	}

	cindex := byte(n.indexForBitPos(idx))

	child := n.Pointers[cindex]
	if child.Key == nil {
		chnd := child.Obj.(*Node)
		chnd.modifyValue(hv, depth+1, k, v)
		return
	}

	switch {
	case *child.Key == k:
		child.Obj = v
	default:
		splnode := NewNode()

		splnode.modifyValue(hv, depth+1, k, v)
		ohv := hash(*child.Key)
		splnode.modifyValue(ohv, depth+1, *child.Key, child.Obj.(string))

		n.setChild(cindex, &Pointer{Obj: splnode})
	}
}

func (n *Node) insertChild(idx int, k, v string) {
	i := n.indexForBitPos(idx)
	n.Bitfield.SetBit(n.Bitfield, idx, 1)

	p := &Pointer{Key: &k, Obj: v}

	n.Pointers = append(n.Pointers[:i], append([]*Pointer{p}, n.Pointers[i:]...)...)
}

func (n *Node) setChild(i byte, p *Pointer) {
	n.Pointers[i] = p
}
