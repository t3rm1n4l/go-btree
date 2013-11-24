package btree

import (
	"fmt"
	"os"
	"testing"
)

const TEST_FILE = "test.tree"

func initTree() *btree {
	f, _ := os.OpenFile(TEST_FILE, os.O_CREATE|os.O_RDWR, os.ModePerm)
	tree := &btree{
		file:   f,
		offset: 0,
	}

	return tree
}

func TestNodeReadWrite(t *testing.T) {
	var n node
	tree := initTree()
	n.ntype = kvnode
	for i := 0; i < 10; i++ {
		itm := new(kv)
		itm.k = Key(fmt.Sprintf("key_%d", i))
		itm.v = Value(fmt.Sprintf("val_%d", i))
		n.kvlist = append(n.kvlist, itm)
	}

	pos, err := tree.writeNode(&n)
	if err != nil {
		t.Fatalf("Failed to write node (%s)", err)
	}

	if pos != 0 {
		t.Fatalf("Invalid pos val %d", pos)
	}

	pos, err = tree.writeNode(&n)
	if err != nil {
		t.Fatalf("Failed to write node (%s)", err)
	}

	if pos == 0 {
		t.Fatalf("Invalid pos val %d", pos)
	}

	m, _ := tree.readNode(pos)
	if m.ntype != kvnode {
		t.Errorf("Invalid node type (%d)", m.ntype)
	}
	if len(m.kvlist) != 10 {
		t.Fatalf("Found different numbers of kvs in node (%d)", len(m.kvlist))
	}

	for i := 0; i < len(m.kvlist); i++ {
		if string(m.kvlist[i].k) != fmt.Sprintf("key_%d", i) {
			t.Fatalf("Found invalid key - %s", string(m.kvlist[i].k))
		}

		if string(m.kvlist[i].v) != fmt.Sprintf("val_%d", i) {
			t.Fatalf("Found invalid key - %s", string(m.kvlist[i].v))
		}
	}
}
