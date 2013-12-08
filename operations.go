package btree

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

	for len(root_builder.pointers) > 1 {
		tmp_builder := new(node_builder)
		tmp_builder.ntype = kpnode
		tmp_builder.tree = nb.tree

		err := tmp_builder.add_vals(root_builder.pointers)
		if err != nil {
			return nil, err
		}
		tmp_builder.finish()

		root_builder = tmp_builder
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
