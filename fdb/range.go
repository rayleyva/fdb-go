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

package fdb

/*
 #define FDB_API_VERSION 100
 #include <foundationdb/fdb_c.h>
*/
import "C"

type KeyValue struct {
	Key, Value []byte
}

type RangeOptions struct {
	Limit int
	Mode StreamingMode
	Reverse bool
}

type RangeResult struct {
	t *Transaction
	begin, end KeySelector
	options RangeOptions
	snapshot bool
	f *FutureKeyValueArray
}

func (rr *RangeResult) GetSliceWithError() ([]KeyValue, error) {
	var ret []KeyValue

	ri := rr.Iterator()

	if rr.options.Limit != 0 {
		ri.options.Mode = StreamingModeExact
	} else {
		ri.options.Mode = StreamingModeWantAll
	}

	for ri.Advance() {
		if ri.err != nil {
			return nil, ri.err
		}
		ret = append(ret, ri.kvs...)
		ri.index = len(ri.kvs)
		ri.fetchNextBatch()
	}

	return ret, nil
}

func (rr *RangeResult) GetSliceOrPanic() []KeyValue {
	kvs, e := rr.GetSliceWithError()
	if e != nil {
		panic(e)
	}
	return kvs
}

func (rr *RangeResult) Iterator() *RangeIterator {
	return &RangeIterator{
		rr: rr,
		f: rr.f,
		begin: rr.begin,
		end: rr.end,
		options: rr.options,
		iteration: 1,
	}
}

type RangeIterator struct {
	rr *RangeResult
	f *FutureKeyValueArray
	begin, end KeySelector
	options RangeOptions
	iteration int
	done bool
	more bool
	kvs []KeyValue
	index int
	err error
}

func (ri *RangeIterator) Advance() bool {
	if ri.done {
		return false
	}

	if ri.f == nil {
		return true
	}

	ri.kvs, ri.more, ri.err = ri.f.GetWithError()
	ri.index = 0
	ri.f = nil
	
	if ri.err != nil || len(ri.kvs) > 0 {
		return true
	}

	return false
}

func (ri *RangeIterator) fetchNextBatch() {
	if !ri.more || ri.index == ri.options.Limit {
		ri.done = true
		return
	}

	if ri.options.Limit > 0 {
		// Not worried about this being zero, checked equality above
		ri.options.Limit -= ri.index
	}

	if ri.options.Reverse {
		ri.end = FirstGreaterOrEqual(ri.kvs[ri.index-1].Key)
	} else {
		ri.begin = FirstGreaterThan(ri.kvs[ri.index-1].Key)
	}

	ri.iteration += 1

	ri.f = ri.rr.t.doGetRange(ri.begin, ri.end, ri.options, ri.rr.snapshot, ri.iteration)
}

func (ri *RangeIterator) GetNextWithError() (kv KeyValue, e error) {
	if ri.err != nil {
		e = ri.err
		return
	}

	kv = ri.kvs[ri.index]

	ri.index += 1

	if ri.index == len(ri.kvs) {
		ri.fetchNextBatch()
	}

	return
}

func (ri *RangeIterator) GetNextOrPanic() KeyValue {
	kv, e := ri.GetNextWithError()
	if e != nil {
		panic(e)
	}
	return kv
}
