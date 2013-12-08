package btree

import (
	"fmt"
	"testing"
)

func TestSimpleTreeBuild(t *testing.T) {
	var n node
	tree := initTree()

	for i := 0; i < 100000; i++ {
		itm := new(kv)
		itm.k = Key(fmt.Sprintf("key_%d", i))
		itm.v = Value(fmt.Sprintf("val_%d", i))
		n.kvlist = append(n.kvlist, itm)
	}

	tree.build(n.kvlist)
	if tree.root == nil {
		t.Fatal("Invalid root")
	}
}
