package btree

import (
	"fmt"
	"os"
)

const (
	TEST_FILE    = "test.tree"
	KP_CHUNKSIZE = 1000
	KV_CHUNKSIZE = 1000
)

func initTree() *btree {
	os.Remove(TEST_FILE)
	f, _ := os.OpenFile(TEST_FILE, os.O_CREATE|os.O_RDWR, os.ModePerm)
	tree := &btree{
		file:   f,
		offset: 0,
		config: Config{KV_CHUNKSIZE, KP_CHUNKSIZE},
	}

	return tree
}

func openTree() *btree {
	f, _ := os.OpenFile(TEST_FILE, os.O_RDONLY, os.ModePerm)
	tree := &btree{
		file:   f,
		offset: 0,
		config: Config{KV_CHUNKSIZE, KP_CHUNKSIZE},
	}

	return tree
}

func make_key(id int) Key {
	return Key(fmt.Sprintf("key_%d", id))
}

func make_value(id int) Value {
	return Value(fmt.Sprintf("val_%d", id))
}

func equals(itm1, itm2 kv) bool {
	if string(itm1.k) == string(itm2.k) && string(itm1.v) == string(itm2.v) {
		return true
	}
	return false
}
