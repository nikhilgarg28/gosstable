package gosstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"sort"
)

func handle(err error) {
	if err != nil {
		panic(err)
	}
}

// ----------------------------------------------
// KeyValue/Dictionary types
// ----------------------------------------------

type KeyValue struct {
	key   []byte
	value []byte
}

func NewKeyValue(key []byte, val []byte) *KeyValue {
	return &KeyValue{key, val}
}

type Dictionary []KeyValue

func (d Dictionary) Len() int {
	return len(d)
}

func (d Dictionary) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d Dictionary) Less(i, j int) bool {
	return bytes.Compare(d[i].key, d[j].key) < 0
}

// ----------------------------------------------
// Table
// ----------------------------------------------

const (
	version    = 0
	blocksize  = 1 << 12 // 4K bytes
	footersize = 16
)

const (
	loading = 1 + iota
	live
)

type Table struct {
	filename string
	status   uint8
	index    []indexEntry
}

type indexEntry struct {
	key    seq
	offset uint64
}

func New(filename string) (t *Table) {
	t = &Table{filename, loading, make([]indexEntry, 0)}
	go t.load()
	return t
}

func (t *Table) Status() uint8 {
	return t.status
}

func (t *Table) Get(k []byte) []byte {
	if t.Status() == loading {
		return nil
	}
	s := newSeq(k)

	// log(N) memory operations where N is the size of index
	block := t.search(s)
	if block == len(t.index) {
		return nil
	}
	offset := t.index[block].offset

	f, err := os.Open(t.filename)
	handle(err)
	defer func() {
		handle(f.Close())
	}()

	f.Seek(int64(offset), 0)

	reader := bufio.NewReader(f)
	for {
		ks := readSeq(reader)
		vs := readSeq(reader)

		if s.equals(ks) {
			return vs.data
		}

		if !ks.less(s) {
			return nil
		}
	}
}

func (t *Table) search(s *seq) int {
	filter := func(i int) bool {
		return s.less(&t.index[i].key)
	}
	return sort.Search(len(t.index), filter)
}

func (t *Table) load() {
	data, err := ioutil.ReadFile(t.filename)
	handle(err)

	n := len(data)

	// let's seek all the way to the end and read footer block
	r := bytes.NewReader(data[n-footersize:])
	var index_offset, ver uint64
	binary.Read(r, binary.LittleEndian, &index_offset)
	binary.Read(r, binary.LittleEndian, &ver)

	// now let's seek to the beginning of the index and read it till the end
	// discarding final 4 bytes which don't belong to the index
	index_buffer := data[index_offset : n-footersize]
	size_index_buffer := len(index_buffer)

	start := 0
	for start < size_index_buffer {
		ks, read := unmarshal(index_buffer[start:])
		start += read
		offset, read := binary.Uvarint(index_buffer[start:])
		start += read
		entry := indexEntry{*ks, offset}
		t.index = append(t.index, entry)
	}

	t.status = live
}

func write(w *bufio.Writer, v []byte) {
	_, err := w.Write(v)
	handle(err)
}

// Table's Write method writes all the key-value pairs in data to a local file
// The format of the file (for version 0) is as follows:
// Data Block 1
// Data Block 2
// ...
// ...
// Data Block N
// Index Block
// Footer
//
// Each data block has following format:
// len(key1) key1 len(val1) val2 len(key2) key2 len(val2) val2
// ...
//
// Index block has the following format:
// len(key1) key1 offset_1 len(key_x) key_x offset_x ...
//
// Footer has following format
// offset_index version
func Write(localfile string, d *Dictionary) {

	// first sort the dictionary so that binary search works
	sort.Sort(d)
	f, err := os.Create(localfile)
	handle(err)

	defer func() {
		handle(f.Close())
	}()

	index := make([]indexEntry, 0)

	// make a buffered writer
	w := bufio.NewWriter(f)

	offset := uint64(0)
	last := len(*d) - 1
	block_start := uint64(0)
	for i, keyValue := range *d {
		k, v := keyValue.key, keyValue.value

		ks, vs := newSeq(k), newSeq(v)
		ksm, vsm := ks.marshal(), vs.marshal()

		write(w, ksm)
		write(w, vsm)

		length := len(ksm) + len(vsm)
		offset += uint64(length)
		if offset-block_start >= blocksize || i == last {
			index = append(index, indexEntry{*ks, block_start})
			block_start = offset
		}
	}

	index_offset := offset

	offset_buffer := make([]byte, 10)
	for _, entry := range index {
		ks, offset := entry.key, entry.offset
		ksm := ks.marshal()
		write(w, ksm)

		n := binary.PutUvarint(offset_buffer, offset)
		write(w, offset_buffer[:n])
	}

	// in the end, add index offset and the version

	binary.Write(w, binary.LittleEndian, index_offset)
	v := uint64(version)
	binary.Write(w, binary.LittleEndian, v)

	w.Flush()
}
