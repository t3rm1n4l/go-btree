package btree

import (
	"bytes"
	"encoding/binary"
	"io"
	"unsafe"
)

// Node types
const (
	kvnode = iota
	kpnode
)

type Key []byte
type Value []byte
type DiskPos int64

type kv struct {
	k Key
	v Value
}

func (itm kv) Size() uint32 {
	sz := unsafe.Sizeof(itm)
	return uint32(sz)
}

// Bytes representation of kv
func (itm *kv) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(len(itm.k)))
	buf.Write(itm.k)
	binary.Write(buf, binary.LittleEndian, uint32(len(itm.v)))
	buf.Write(itm.v)

	return buf.Bytes()
}

// Read kv from file
func (itm *kv) Read(r io.Reader) error {
	var l uint32
	var buf []byte

	err := binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return err
	}

	buf = make([]byte, l)
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	itm.k = Key(buf)

	err = binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return err
	}

	buf = make([]byte, l)
	_, err = r.Read(buf[:l])
	if err != nil {
		return err
	}
	itm.v = Value(buf[:l])

	return nil
}

type node struct {
	ntype  int8
	kvlist []*kv
}

// Read from diskpos and parse node
func (tree *btree) readNode(pos int64) (*node, error) {
	var l uint32
	n := new(node)

	tree.file.Seek(pos, 0)
	err := binary.Read(tree.file, binary.LittleEndian, &n.ntype)
	if err != nil {
		return nil, err
	}
	err = binary.Read(tree.file, binary.LittleEndian, &l)
	if err != nil {
		return nil, err
	}

	for i := 0; i < int(l); i++ {
		itm := new(kv)
		err = itm.Read(tree.file)
		if err != nil {
			return nil, err
		}
		n.kvlist = append(n.kvlist, itm)
	}

	return n, nil
}

// Write node to disk and return diskpos
func (tree *btree) writeNode(n *node) (pos int64, err error) {
	var written int
	offset := tree.offset
	pos = offset

	err = binary.Write(tree.file, binary.LittleEndian, n.ntype)
	if err != nil {
		return
	}
	err = binary.Write(tree.file, binary.LittleEndian, uint32(len(n.kvlist)))
	if err != nil {
		return
	}

	offset += int64(5)

	for i := 0; i < len(n.kvlist); i++ {
		written, err = tree.file.Write(n.kvlist[i].Bytes())
		if err != nil {
			return
		}
		offset += int64(written)
	}

	tree.offset = offset
	return
}

func v2p(v Value) int64 {
	var pos int64
	buf := bytes.NewBuffer(v)
	binary.Read(buf, binary.LittleEndian, &pos)
	return pos
}

func p2v(p int64) Value {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, p)
	return Value(buf.Bytes())
}
