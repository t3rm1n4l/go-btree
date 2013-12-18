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

func TestSimpleTreeLookup(t *testing.T) {
	var n node
	tree := initTree()

	for i := 0; i < 1000; i++ {
		itm := new(kv)
		itm.k = Key(fmt.Sprintf("key_%d", i))
		itm.v = Value(fmt.Sprintf("val_%d", i))
		n.kvlist = append(n.kvlist, itm)
	}

	tree.build(n.kvlist)
	if tree.root == nil {
		t.Fatal("Invalid root")
	}

	tree.cmp = func(k1, k2 Key) int {
		s1 := string(k1)
		s2 := string(k2)
		i1 := 0
		i2 := 0
		fmt.Sscanf(s1, "key_%d", &i1)
		fmt.Sscanf(s2, "key_%d", &i2)

		return i1 - i2
	}

	key_ids := []int{0, 10, 50, 89, 150, 444, 678, 999, 10000}
	expected := []bool{true, true, true, true, true, true, true, true, false}
	received := []kv{}

	keys := []Key{}
	for i := 0; i < len(key_ids); i++ {
		keys = append(keys, make_key(key_ids[i]))
	}

	vals := []Value{}
	for i := 0; i < len(key_ids); i++ {
		switch {
		case expected[i] == true:
			vals = append(vals, make_value(key_ids[i]))
		default:
			vals = append(vals, Value(""))
		}
	}

	qreq := &QueryRequest{
		Keys: keys,
		Callback: func(itm kv) {
			received = append(received, itm)
		},
	}

	err := tree.query(qreq)
	if err != nil {
		t.Fatal("query returned non-nil error")
	}

	if len(received) != len(vals) {
		t.Fatal("query resulted in missing values")
	}

	for i := 0; i < len(vals); i++ {
		if !equals(received[i], kv{keys[i], vals[i]}) {
			t.Errorf("received %s/%s - expected %s/%s\n",
				string(received[i].k),
				string(received[i].v),
				string(keys[i]),
				string(vals[i]))
		}
	}

}

func TestSimpleTreeRangeLookup(t *testing.T) {
	var n node
	tree := initTree()

	for i := 0; i < 1000; i++ {
		itm := new(kv)
		itm.k = Key(fmt.Sprintf("key_%d", i))
		itm.v = Value(fmt.Sprintf("val_%d", i))
		n.kvlist = append(n.kvlist, itm)
	}

	tree.build(n.kvlist)
	if tree.root == nil {
		t.Fatal("Invalid root")
	}

	tree.cmp = func(k1, k2 Key) int {
		s1 := string(k1)
		s2 := string(k2)
		i1 := 0
		i2 := 0
		fmt.Sscanf(s1, "key_%d", &i1)
		fmt.Sscanf(s2, "key_%d", &i2)

		return i1 - i2
	}

	received := []kv{}

	qreq := &QueryRequest{
		Keys: []Key{Key("key_40"), Key("key_60"), Key("key_80"), Key("key_95")},
		Callback: func(itm kv) {
			received = append(received, itm)
		},
		Range: true,
	}

	err := tree.query(qreq)
	if err != nil {
		t.Fatal("query returned non-nil error")
	}

	for i := 0; i <= 20; i++ {
		offset := i + 40
		itm := kv{make_key(offset), make_value(offset)}
		if !equals(itm, received[i]) {
			t.Errorf("Unexpected key received, %s for %s", string(itm.k), string(received[i].k))
		}
	}

	for i := 21; i <= 21+15; i++ {
		offset := i + 59
		itm := kv{make_key(offset), make_value(offset)}
		if !equals(itm, received[i]) {
			t.Errorf("Unexpected key received, %s for %s", string(itm.k), string(received[i].k))
		}
	}
}
