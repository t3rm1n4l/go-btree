package btree

import (
	"errors"
)

type node_builder struct {
	ntype    int8
	tree     *btree
	pointers []*kv
	values   []*kv
	valsize  uint32
}

func new_node_builder(tree *btree, t int8) *node_builder {
	return &node_builder{ntype: t, tree: tree, valsize: 0}
}

// max chunk size
func (b node_builder) chunkSize() uint32 {
	if b.ntype == kvnode {
		return b.tree.config.kvChunkSize
	}

	return b.tree.config.kpChunkSize
}

// Write out current kvs and get diskpos
func (b *node_builder) flush() error {
	n := &node{ntype: b.ntype, kvlist: b.values}
	pos, err := b.tree.writeNode(n)
	if err != nil {
		return err
	}

	itm := &kv{k: b.values[len(b.values)-1].k, v: p2v(pos)}
	b.values = *new([]*kv)
	b.pointers = append(b.pointers, itm)
	b.valsize = 0

	return err
}

// If any kvs left, writeout and get diskpos
func (b *node_builder) finish() error {
	var err error

	if len(b.values) > 0 {
		err = b.flush()
	}

	return err
}

// If accumulated enough kvs > chunksize, writeout
func (b *node_builder) maybe_flush() error {

	if b.valsize >= b.chunkSize() {
		err := b.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

// Add an item to builder
func (b *node_builder) add(itm *kv) error {
	b.values = append(b.values, itm)
	b.valsize += itm.Size()

	return b.maybe_flush()
}

// Add vals from one builder to another
func (b *node_builder) add_vals(vals []*kv) error {
	for _, itm := range vals {
		err := b.add(itm)
		if err != nil {
			return err
		}
	}

	return nil
}

// Reduce to single node by generating levels of nodes
func build_root(nb *node_builder) (*node, error) {
	root_builder := nb
	nb.finish()

	for {
		tmp_builder := new(node_builder)
		tmp_builder.ntype = kpnode
		tmp_builder.tree = nb.tree

		err := tmp_builder.add_vals(root_builder.pointers)
		if err != nil {
			return nil, err
		}

		tmp_builder.finish()
		root_builder = tmp_builder
		if len(root_builder.pointers) == 1 {
			break
		}
	}

	root := new(node)
	root.ntype = kpnode
	root.kvlist = append(root.kvlist, root_builder.pointers[0])

	return root, nil
}

// Build a btree from sorted kv items
func (tree *btree) build(kvs []*kv) error {
	var nb node_builder
	var err error
	nb.ntype = kvnode
	nb.tree = tree

	for _, itm := range kvs {
		err := nb.add(itm)
		if err != nil {
			return err
		}
	}

	tree.root, err = build_root(&nb)
	return err
}

// Query request spec
type QueryRequest struct {
	// Sorted keylist
	Keys []*Key
	// If this is range query
	Range        bool
	rangeStarted bool
	noaction     bool
	// Fetch callback
	Callback func(itm kv)
}

// Query api
func (tree *btree) query(rq *QueryRequest) error {
	if tree.root == nil {
		return errors.New("Empty root")
	}

	return tree.query_node(rq, v2p(tree.root.kvlist[0].v), 0, len(rq.Keys))
}

func (tree *btree) query_node(rq *QueryRequest, diskPos int64, start, end int) error {
	n, err := tree.readNode(diskPos)
	if err != nil {
		return err
	}

	max := len(n.kvlist)
	// If it is kpnode, descend to the appropriate node with search subgroup keys
	if n.ntype == kpnode {
		for i := 0; (rq.rangeStarted || start < end) && i < max; i++ {
			cmpkey := n.kvlist[i]
			cmpval := 0
			switch {
			case rq.Keys[start] == nil:
				cmpval = -1
				if !rq.rangeStarted {
					rq.rangeStarted = true
					start++
				}
				fallthrough
			default:
				if rq.Keys[start] != nil {
					cmpval = tree.cmp(&cmpkey.k, rq.Keys[start])
				}
			}

			switch {
			case cmpval >= 0:
				last := start
				for last < end && rq.Keys[last] != nil && tree.cmp(&cmpkey.k, rq.Keys[last]) >= 0 {
					last++
				}

				err := tree.query_node(rq, v2p(cmpkey.v), start, last)
				if err != nil {
					return err
				}

				start = last
				break
			case rq.rangeStarted:
				rq.noaction = true
				err := tree.query_node(rq, v2p(cmpkey.v), 1, 2)
				if err != nil {
					return err
				}
				rq.noaction = false
			}

		}

		for !rq.Range && start < end {
			not_found := kv{*rq.Keys[start], Value("")}
			rq.Callback(not_found)
			start++
		}
	}

	// Search for given list of keys in kvnode
	if n.ntype == kvnode {
		for i := 0; (rq.rangeStarted || start < end) && i < max; i++ {
			cmpkey := n.kvlist[i]
			cmpval := 0
			switch {
			case rq.Keys[start] == nil:
				cmpval = -1
			default:
				cmpval = tree.cmp(&cmpkey.k, rq.Keys[start])
			}

			switch {
			case !rq.noaction && cmpval > 0:
				switch {
				case !rq.Range:
					not_found := kv{*rq.Keys[start], Value("")}
					rq.Callback(not_found)
					break
				default:
					if rq.Range {
						switch {
						case rq.rangeStarted:
							rq.rangeStarted = false
							break
						default:
							rq.rangeStarted = true
							rq.Callback(*cmpkey)
						}
					}
				}
				start++
				break
			case !rq.noaction && cmpval == 0:
				rq.Callback(*cmpkey)
				start++
				if rq.Range {
					switch {
					case rq.rangeStarted:
						rq.rangeStarted = false
						break
					default:
						rq.rangeStarted = true
					}
				}
				break

			case rq.rangeStarted:
				rq.Callback(*cmpkey)
				break
			}
		}
	}

	return nil
}

const (
	OP_INSERT = iota
	OP_DELETE
)

type Operation struct {
	itm kv
	op  int
}

type ModifyRequest struct {
	ops []Operation
}

func (tree *btree) modify(rq *ModifyRequest) error {
	root_builder := new_node_builder(tree, kpnode)
	err := tree.modify_node(rq, root_builder, v2p(tree.root.kvlist[0].v), 0, len(rq.ops))
	if err != nil {
		return err
	}

	tree.root, err = build_root(root_builder)
	return err
}

func (tree *btree) modify_node(rq *ModifyRequest, nb *node_builder, diskPos int64, start, end int) error {
	n, err := tree.readNode(diskPos)
	if err != nil {
		return err
	}

	cnb := new_node_builder(tree, n.ntype)

	max := len(n.kvlist)
	i := 0

	if n.ntype == kpnode {
		for ; start < end && i < max; i++ {
			cmpkey := n.kvlist[i].k
			cmpval := tree.cmp(&cmpkey, &rq.ops[start].itm.k)
			if i == max-1 {
				err = tree.modify_node(rq, cnb, v2p(n.kvlist[i].v), start, end)
				if err != nil {
					return err
				}
				break
			}

			switch {
			case cmpval < 0:
				cnb.add(n.kvlist[i])
			case cmpval >= 0:
				range_end := start
				for range_end < end && tree.cmp(&cmpkey, &rq.ops[range_end].itm.k) >= 0 {
					range_end += 1
				}

				err = tree.modify_node(rq, cnb, v2p(n.kvlist[i].v), start, range_end)
				if err != nil {
					return err
				}
				start = range_end
			}
		}

		for i++; i < max; i++ {
			cnb.add(n.kvlist[i])
		}
	}

	if n.ntype == kvnode {
		for start < end && i < max {
			cmpkey := n.kvlist[i].k
			op := rq.ops[start]
			cmpval := tree.cmp(&cmpkey, &op.itm.k)
			switch {
			case cmpval < 0:
				cnb.add(n.kvlist[i])
				i++
				break
			case cmpval > 0:
				if op.op == OP_INSERT {
					cnb.add(&op.itm)
				}
				start++
				break
			case cmpval == 0:
				if op.op == OP_INSERT {
					cnb.add(&op.itm)
				}
				start++
				i++
				break
			}
		}

		for ; start == end && i < max; i++ {
			cnb.add(n.kvlist[i])
		}

		for ; start < end; start++ {
			op := rq.ops[start]
			if op.op == OP_INSERT {
				cnb.add(&op.itm)
			}
		}
	}

	cnb.finish()
	nb.add_vals(cnb.pointers)

	return err
}

func (tree *btree) write_header() error {
	var err error
	if tree.root == nil {
		return errors.New("Btree root is empty")
	}
	var h header
	h.rootptr, err = tree.writeNode(tree.root)
	if err != nil {
		errors.New("Unable to write root node")
	}

	headerpos := tree.offset + (BLOCK_SIZE - (tree.offset % BLOCK_SIZE))
	tree.file.Seek(headerpos, 0)
	n, err := tree.file.Write(h.Bytes())
	if err != nil {
		return err
	}

	tree.offset = headerpos + int64(n)

	return nil
}

func (tree *btree) read_header() error {
	var err error
	var pos int64
	var h header
	buf := make([]byte, HEADER_SIZE)
	tree.offset, err = tree.file.Seek(0, 2)
	if err != nil {
		return err
	}

	pos = tree.offset
	for {
		diff := pos % BLOCK_SIZE
		pos := pos - diff
		tree.file.Seek(pos, 0)
		tree.file.Read(buf[0:HEADER_SIZE])
		err = h.Parse(buf)
		var root *node
		if err == nil {
			root, err = tree.readNode(h.rootptr)
			if err != nil {
				return err
			}
			tree.root = root
			break
		}

		pos--

		if pos < 0 {
			return errors.New("Btree header not found")
		}
	}

	return nil
}
