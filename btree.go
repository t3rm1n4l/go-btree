package btree
// An MVCC Btree implementation

import (
	"io"
)

type Key []byte
type Value []byte

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
