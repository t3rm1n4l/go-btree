package btree

// An MVCC Btree implementation

import (
	"io"
	"os"
)

type BtreeIter interface {
	HasNext() bool
	Next() (Key, Value)
}

type Btree interface {
	Open(io.Writer) error
	Close() error
	Flush() error
	SetComparator(cmp func(Key, Key) int)
	Insert(Key, Value) error
	Remove(Key) error
	Get(Key) (Value, error)
	Iterator() BtreeIter
}

type Config struct {
	kvChunkSize uint32
	kpChunkSize uint32
}

type btree struct {
	file   *os.File
	offset int64
	config Config
	root   *node
	cmp    func(Key, Key) int
}
