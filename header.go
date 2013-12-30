package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	HEADER_SIZE = 4 + 8
	BLOCK_SIZE  = 4096
)

type header struct {
	rootptr int64
}

func (h *header) Bytes() []byte {
	diskbuf := new(bytes.Buffer)
	content := new(bytes.Buffer)
	binary.Write(content, binary.LittleEndian, h.rootptr)

	cksum := crc32.ChecksumIEEE(content.Bytes())
	binary.Write(diskbuf, binary.LittleEndian, cksum)
	binary.Write(diskbuf, binary.LittleEndian, content.Bytes())

	return diskbuf.Bytes()
}

func (h *header) Parse(b []byte) error {
	var cksum uint32
	diskbuf := bytes.NewBuffer(b)

	binary.Read(diskbuf, binary.LittleEndian, &cksum)
	binary.Read(diskbuf, binary.LittleEndian, &h.rootptr)
	content := new(bytes.Buffer)
	binary.Write(content, binary.LittleEndian, h.rootptr)
	cksum_calc := crc32.ChecksumIEEE(content.Bytes())

	if cksum_calc != cksum {
		return errors.New("Header checksum mismatch")
	}

	return nil
}
