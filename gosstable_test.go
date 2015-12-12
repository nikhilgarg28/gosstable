package gosstable

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// global variable, to ensure compiler can't mess up with benchmarks
var result int

func getDictionary(n, min_key_len, max_key_len, min_val_len, max_val_len int) *Dictionary {
	var d Dictionary
	for i := 0; i < n; i++ {
		key_len := rand.Intn(max_key_len-min_key_len) + min_key_len
		val_len := rand.Intn(max_val_len) + min_val_len
		k := make([]byte, key_len)
		crand.Read(k)

		v := make([]byte, val_len)
		crand.Read(v)

		kv := NewKeyValue(k, v)
		d = append(d, *kv)
	}

	return &d
}

func TestWriteGet(t *testing.T) {
	d := getDictionary(10000, 20, 1000, 20, 1000)
	filename := "test.sstb"
	Write(filename, d)

	// now load the table and see if we can read all the keys back
	table := New(filename)

	// wait till we read the table
	for table.Status() == loading {
		time.Sleep(1 * time.Millisecond)
	}

	for _, kv := range *d {
		k, v := kv.key, kv.value
		ret := table.Get(k)

		if !bytes.Equal(v, ret) {
			t.Errorf("Can not retrive stored value for a key")
		}
	}

	// let's try to retrive keys that don't exist
	d2 := getDictionary(10000, 20, 1000, 20, 1000)

	for _, kv2 := range *d2 {
		k := kv2.key
		ret := table.Get(k)
		if ret != nil {
			// we found a match in table, when we didn't expect to
			// before declaring it as an error, let's verify that we didn't
			// get a random sequence by chance
			found := false
			for _, kv := range *d {
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

func benchmarkWrite(i int, b *testing.B) {
	d := getDictionary(i, 20, 100, 20, 200)
	b.ResetTimer()

	filename := "benchmark.sstb"

	for i := 0; i < b.N; i++ {
		Write(filename, d)
	}
}

func BenchmarkWrite10K(b *testing.B)     { benchmarkWrite(10000, b) }
func BenchmarkWrite100K(b *testing.B)    { benchmarkWrite(100000, b) }
func BenchmarkWriteMillion(b *testing.B) { benchmarkWrite(1000000, b) }

func benchmarkLoad(i int, b *testing.B) {
	d := getDictionary(i, 20, 100, 20, 200)

	for i := 0; i < b.N; i++ {
		// we use a different file each time to bypass file system caching
		b.StopTimer()
		filename := fmt.Sprintf("benchmark_%d.sstb", i)
		Write(filename, d)
		b.StartTimer()
		table := New(filename)

		// wait till we read the table
		for table.Status() == loading {
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func BenchmarkLoad10K(b *testing.B)     { benchmarkLoad(10000, b) }
func BenchmarkLoad100K(b *testing.B)    { benchmarkLoad(100000, b) }
func BenchmarkLoadMillion(b *testing.B) { benchmarkLoad(1000000, b) }

func benchmarkReadHit(s int, b *testing.B) {
	d := getDictionary(s, 20, 100, 20, 200)
	filename := "benchmark.sstb"
	Write(filename, d)
	table := New(filename)
	// wait till we read the table
	for table.Status() == loading {
		time.Sleep(1 * time.Millisecond)
	}
	dict := *d

	indices := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		indices[i] = rand.Intn(s)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := indices[i]
		key := dict[idx].key
		v := table.Get(key)
		result += len(v)
	}
}

func BenchmarkReadHit10K(b *testing.B)     { benchmarkReadHit(10000, b) }
func BenchmarkReadHit100K(b *testing.B)    { benchmarkReadHit(100000, b) }
func BenchmarkReadHitMillion(b *testing.B) { benchmarkReadHit(1000000, b) }

func benchmarkReadMiss(s int, b *testing.B) {
	d := getDictionary(s, 20, 100, 20, 200)
	filename := "benchmark.sstb"
	Write(filename, d)
	table := New(filename)
	// wait till we read the table
	for table.Status() == loading {
		time.Sleep(1 * time.Millisecond)
	}

	// getting another random dictionary ensures that there's almost
	// zero overlap between table's contents and new keys, resulting in most
	// misses
	d2 := getDictionary(b.N, 20, 100, 20, 200)
	dict := *d2

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := dict[i].key
		v := table.Get(key)
		result += len(v)
	}
}

func BenchmarkReadMiss10K(b *testing.B)     { benchmarkReadMiss(10000, b) }
func BenchmarkReadMiss100K(b *testing.B)    { benchmarkReadMiss(100000, b) }
func BenchmarkReadMissMillion(b *testing.B) { benchmarkReadMiss(1000000, b) }
