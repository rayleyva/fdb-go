// FoundationDB Go API
// Copyright (c) 2013 FoundationDB, LLC

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package fdb_test

import (
	"github.com/FoundationDB/fdb-go/fdb"
	"fmt"
)

func ExamplePrefixRange() {
	_ = fdb.APIVersion(100)
	db, _ := fdb.OpenDefault()
	tr, _ := db.CreateTransaction()

	// Clear and initialize data in this transaction. In examples we do not
	// commit transactions to avoid mutating a real database.
	tr.ClearRange(nil, []byte{0xFF})
	tr.Set([]byte("alpha"), []byte("1"))
	tr.Set([]byte("alphabetA"), []byte("2"))
	tr.Set([]byte("alphabetB"), []byte("3"))
	tr.Set([]byte("alphabetize"), []byte("4"))
	tr.Set([]byte("beta"), []byte("5"))

	// Construct the range of all keys beginning with "alphabet"
	begin, end, _ := fdb.PrefixRange([]byte("alphabet"))
	kvs, _ := tr.GetRange(begin, end, fdb.RangeOptions{}).GetSliceWithError()

	for _, kv := range kvs {
		fmt.Printf("%s: %s\n", string(kv.Key), string(kv.Value))
	}

	// Output:
	// alphabetA: 2
	// alphabetB: 3
	// alphabetize: 4
}

func ExampleRangeIterator() {
	_ = fdb.APIVersion(100)
	db, _ := fdb.OpenDefault()
	tr, _ := db.CreateTransaction()

	// Clear and initialize data in this transaction. In examples we do not
	// commit transactions to avoid mutating a real database.
	tr.ClearRange(nil, []byte{0xFF})
	tr.Set([]byte("apple"), []byte("foo"))
	tr.Set([]byte("cherry"), []byte("baz"))
	tr.Set([]byte("banana"), []byte("bar"))

	rr := tr.GetRange([]byte(""), []byte{0xFF}, fdb.RangeOptions{})
	ri := rr.Iterator()

	// Advance() will return true until the iterator is exhausted
	for ri.Advance() {
		kv, _ := ri.GetNextWithError()
		fmt.Printf("%s is %s\n", kv.Key, kv.Value)
	}

	// Output:
	// apple is foo
	// banana is bar
	// cherry is baz
}
