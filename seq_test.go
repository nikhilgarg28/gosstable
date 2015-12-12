package gosstable

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestnewSeq(t *testing.T) {
	b := []byte("Some sequence")
	l := len(b)

	s := newSeq(b)
	sm := s.marshal()
	n := len(sm)

	if !bytes.Equal(sm[n-l:], b) {
		t.Errorf("Marshalled sequence should end with original sequence")
	}

	num, read := binary.Uvarint(sm)

	if num != uint64(l) {
		t.Errorf("Marshalled sequence should start with length of sequence")
	}

	if read != n-l {
		t.Errorf("Prefix of marshalled sequence of wrong length")
	}
}

func TestMarshallUnMarshall(t *testing.T) {
	b1 := []byte("Some sequence")
	s1 := newSeq(b1)
	sm := s1.marshal()

	s2, n2 := unmarshal(sm)

	if !s1.equals(s2) {
		t.Errorf("Reading marshalled seq should be same as original sequence")
	}

	sm_extended := append(sm, b1...)
	s3, n3 := unmarshal(sm_extended)

	if !s1.equals(s3) {
		t.Errorf("Reading marshalled seq should be same as original sequence")
	}
	if n2 != n3 {
		t.Errorf("Not reading the correct length")
	}
}

func TestLessEquals(t *testing.T) {
	b1 := []byte("Some sequence")
	s1 := newSeq(b1)

	if !s1.less(s1) {
		t.Errorf("All sequences should be less than equal to themselves.")
	}

	b2 := []byte("one more byte sequence")
	s2 := newSeq(b2)

	s1_s2 := s1.less(s2)
	s2_s1 := s2.less(s1)

	if s1_s2 == s2_s1 {
		t.Errorf("Two unequal sequences can't be both less than each other.")
	}

	if s1.equals(s2) {
		t.Errorf("Two unequal sequences shouldn't be equal.")
	}

	b3 := []byte{0x18, 0x2d}
	s3 := newSeq(b3)
	b4 := []byte{0x28, 0x2d, 0x0}
	s4 := newSeq(b4)

	if !s3.less(s4) {
		t.Errorf("Small sequence should be less than the other one")
	}
}
