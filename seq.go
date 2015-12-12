package gosstable

import (
	"bytes"
	"encoding/binary"
)

type seq struct {
	data []byte
}

func clone(b1 []byte) (b2 []byte) {
	l := len(b1)
	b2 = make([]byte, l)
	copy(b2, b1)
	return b2
}

func newSeq(b []byte) *seq {
	return &seq{b}
}

func (s *seq) marshal() []byte {
	length := uint64(len(s.data))
	// varint for all 64bit integers fit within 10 bytes
	length_buffer := make([]byte, 10)
	n := binary.PutUvarint(length_buffer, length)
	return append(length_buffer[:n], s.data...)
}

func unmarshal(b []byte) (s *seq, n int) {
	length, n := binary.Uvarint(b)
	data := make([]byte, length)
	for i := uint64(0); i < length; i++ {
		data[i] = b[n]
		n += 1
	}

	return newSeq(data), n
}

type StreamReader interface {
	Read(p []byte) (n int, err error)
	ReadByte() (c byte, err error)
}

func readSeq(r StreamReader) *seq {
	length, _ := binary.ReadUvarint(r)
	data := make([]byte, length)
	binary.Read(r, binary.LittleEndian, &data)
	return newSeq(data)
}

func (s1 *seq) less(s2 *seq) bool {
	return bytes.Compare(s1.data, s2.data) <= 0
}

func (s1 *seq) equals(s2 *seq) bool {
	return bytes.Compare(s1.data, s2.data) == 0
}
