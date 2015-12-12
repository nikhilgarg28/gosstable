package gosstable

import (
	"bytes"
	crand "crypto/rand"
	"math/rand"
	"testing"
	"time"
)

func TestWriteGet(t *testing.T) {
	var d Dictionary
	size := 10000
	for i := 0; i < size; i++ {
		key_len := rand.Intn(1000) + 20
		val_len := rand.Intn(1000) + 20
		k := make([]byte, key_len)
		crand.Read(k)

		v := make([]byte, val_len)
		crand.Read(v)

		kv := NewKeyValue(k, v)
		d = append(d, *kv)
	}

	filename := "test.tbl"
	Write(filename, &d)

	// now load the table and see if we can read all the keys back
	table := New(filename)

	// wait till we read the table
	for table.Status() == loading {
		time.Sleep(1 * time.Millisecond)
	}

	for _, kv := range d {
		k, v := kv.key, kv.value
		ret := table.Get(k)

		if !bytes.Equal(v, ret) {
			t.Errorf("Can not retrive stored value for a key")
		}
	}

	// let's try to retrive keys that don't exist
	for i := 0; i < size; i++ {
		key_len := rand.Intn(1000) + 20
		k := make([]byte, key_len)
		crand.Read(k)

		ret := table.Get(k)
		if ret != nil {
			// we found a match in table, when we didn't expect to
			// before declaring it as an error, let's verify that we didn't
			// get a random sequence by chance
			found := false
			for _, kv := range d {
				if bytes.Equal(kv.key, k) {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Did not get nil back for a non-existent key")
			}
		}
	}
}
