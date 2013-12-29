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

	tree.cmp = func(k1, k2 *Key) int {
		s1 := string(*k1)
		s2 := string(*k2)
		i1 := 0
		i2 := 0
		fmt.Sscanf(s1, "key_%d", &i1)
		fmt.Sscanf(s2, "key_%d", &i2)

		return i1 - i2
	}

	key_ids := []int{0, 10, 50, 89, 150, 444, 678, 999, 10000}
	expected := []bool{true, true, true, true, true, true, true, true, false}
	received := []kv{}

	keys := []*Key{}
	for i := 0; i < len(key_ids); i++ {
		k := make_key(key_ids[i])
		keys = append(keys, &k)
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
		if !equals(received[i], kv{*keys[i], vals[i]}) {
			t.Errorf("received %s/%s - expected %s/%s\n",
				string(received[i].k),
				string(received[i].v),
				string(*keys[i]),
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

	tree.cmp = func(k1, k2 *Key) int {
		s1 := string(*k1)
		s2 := string(*k2)
		i1 := 0
		i2 := 0
		fmt.Sscanf(s1, "key_%d", &i1)
		fmt.Sscanf(s2, "key_%d", &i2)

		return i1 - i2
	}

	received := []kv{}
	k1 := new(Key)
	*k1 = Key("key_40")
	k2 := new(Key)
	*k2 = Key("key_60")
	k3 := new(Key)
	*k3 = Key("key_80")
	k4 := new(Key)
	*k4 = Key("key_95")

	qreq := &QueryRequest{
		Keys: []*Key{k1, k2, k3, k4},
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

func TestSimpleTreeRangeLookup2(t *testing.T) {
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

	tree.cmp = func(k1, k2 *Key) int {
		s1 := string(*k1)
		s2 := string(*k2)
		i1 := 0
		i2 := 0
		fmt.Sscanf(s1, "key_%d", &i1)
		fmt.Sscanf(s2, "key_%d", &i2)

		return i1 - i2
	}

	received := []kv{}
	var k1 *Key = nil
	k2 := new(Key)
	*k2 = Key("key_100")
	k3 := new(Key)
	*k3 = Key("key_900")
	var k4 *Key = nil

	qreq := &QueryRequest{
		Keys: []*Key{k1, k2, k3, k4},
		Callback: func(itm kv) {
			received = append(received, itm)
		},
		Range: true,
	}

	err := tree.query(qreq)
	if err != nil {
		t.Fatal("query returned non-nil error")
	}

	for i := 0; i <= 100; i++ {
		itm := kv{make_key(i), make_value(i)}
		if !equals(itm, received[i]) {
			t.Errorf("Unexpected key received, %s for %s", string(itm.k), string(received[i].k))
		}
	}

	for i := 101; i <= 101+99; i++ {
		offset := i + 799
		itm := kv{make_key(offset), make_value(offset)}
		if !equals(itm, received[i]) {
			t.Errorf("Unexpected key received, %s for %s", string(itm.k), string(received[i].k))
		}
	}
}

func TestSimpleTreeFullLookup(t *testing.T) {
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

	tree.cmp = func(k1, k2 *Key) int {
		s1 := string(*k1)
		s2 := string(*k2)
		i1 := 0
		i2 := 0
		fmt.Sscanf(s1, "key_%d", &i1)
		fmt.Sscanf(s2, "key_%d", &i2)

		return i1 - i2
	}

	received := []kv{}

	qreq := &QueryRequest{
		Keys: []*Key{nil, nil},
		Callback: func(itm kv) {
			received = append(received, itm)
		},
		Range: true,
	}

	err := tree.query(qreq)
	if err != nil {
		t.Fatal("query returned non-nil error")
	}

	for i := 0; i < 1000; i++ {
		itm := kv{make_key(i), make_value(i)}
		if !equals(itm, received[i]) {
			t.Errorf("Unexpected key received, %s for %s", string(itm.k), string(received[i].k))
		}
	}
}

func TestSimpleTreeModify(t *testing.T) {
	var n node
	N := 20
	tree := initTree()

	for i := 0; i < N; i += 2 {
		itm := new(kv)
		itm.k = Key(fmt.Sprintf("key_%d", i))
		itm.v = Value(fmt.Sprintf("val_%d", i))
		n.kvlist = append(n.kvlist, itm)
	}

	tree.build(n.kvlist)
	if tree.root == nil {
		t.Fatal("Invalid root")
	}

	tree.cmp = func(k1, k2 *Key) int {
		s1 := string(*k1)
		s2 := string(*k2)
		i1 := 0
		i2 := 0
		fmt.Sscanf(s1, "key_%d", &i1)
		fmt.Sscanf(s2, "key_%d", &i2)

		return i1 - i2
	}

	rq := &ModifyRequest{
		ops: []Operation{
			Operation{itm: kv{make_key(4), make_value(4)}, op: OP_DELETE},
			Operation{itm: kv{make_key(9), make_value(9)}, op: OP_INSERT},
			Operation{itm: kv{make_key(12), make_value(100)}, op: OP_INSERT},
			Operation{itm: kv{make_key(25), make_value(25)}, op: OP_INSERT},
		},
	}

	err := tree.modify(rq)
	if err != nil {
		t.Fatal("modify returned non-nil error", err)
	}

	received := []kv{}

	qreq := &QueryRequest{
		Keys: []*Key{nil, nil},
		Callback: func(itm kv) {
			received = append(received, itm)
		},
		Range: true,
	}

	err = tree.query(qreq)
	if err != nil {
		t.Fatal("query returned non-nil error", err)
	}

	if len(received) != N/2+1 {
		t.Error("Unexpected count after modification", len(received))
	}

	k1 := make_key(4)
	k2 := make_key(9)
	k3 := make_key(12)
	k4 := make_key(25)

	qreq2 := &QueryRequest{
		Keys: []*Key{&k1, &k2, &k3, &k4},
		Callback: func(itm kv) {
			received = append(received, itm)
		},
	}

	received = []kv{}
	err = tree.query(qreq2)
	if err != nil {
		t.Fatal("query returned non-nil error", err)
	}

	if len(received) != 4 {
		t.Error("Unexpected count after modification")
	}
	if !equals(received[0], kv{k1, Value("")}) {
		t.Errorf("Deleted key found", string(received[0].v))
	}

	if !equals(received[1], kv{k2, make_value(9)}) {
		t.Errorf("Error inserted key with unexpected val")
	}

	if !equals(received[2], kv{k3, make_value(100)}) {
		t.Errorf("Error inserted key with unexpected val")
	}

	if !equals(received[3], kv{k4, make_value(25)}) {
		t.Errorf("Error inserted key with unexpected val")
	}

}
