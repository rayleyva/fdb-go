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

import (
	"unsafe"
	"runtime"
)

type Transaction struct {
	t *C.FDBTransaction
}

func (t *Transaction) destroy() {
	C.fdb_transaction_destroy(t.t)
}

func (t *Transaction) OnError(e FDBError) *FutureNil {
	return makeFutureNil(C.fdb_transaction_on_error(t.t, e.Code))
}

func (t *Transaction) Commit() *FutureNil {
	return makeFutureNil(C.fdb_transaction_commit(t.t))
}

func (t *Transaction) Watch(key []byte) *FutureNil {
	return makeFutureNil(C.fdb_transaction_watch(t.t, (*C.uint8_t)(unsafe.Pointer(&key[0])), C.int(len(key))))
}

func (t *Transaction) Get(key []byte) *FutureValue {
	v := &FutureValue{future: future{f: C.fdb_transaction_get(t.t, (*C.uint8_t)(unsafe.Pointer(&key[0])), C.int(len(key)), 0)}}
	runtime.SetFinalizer(v, (*FutureValue).destroy)
	return v
}

func (t *Transaction) Set(key []byte, value []byte) {
	C.fdb_transaction_set(t.t, (*C.uint8_t)(unsafe.Pointer(&key[0])), C.int(len(key)), (*C.uint8_t)(unsafe.Pointer(&value[0])), C.int(len(value)))
}

func (t *Transaction) Clear(key []byte) {
	C.fdb_transaction_clear(t.t, (*C.uint8_t)(unsafe.Pointer(&key[0])), C.int(len(key)))
}

func (t *Transaction) ClearRange(begin []byte, end []byte) {
	C.fdb_transaction_clear_range(t.t, (*C.uint8_t)(unsafe.Pointer(&begin[0])), C.int(len(begin)), (*C.uint8_t)(unsafe.Pointer(&end[0])), C.int(len(end)))
}

func (t *Transaction) GetCommittedVersion() (int64, error) {
	var version C.int64_t
	if err := C.fdb_transaction_get_committed_version(t.t, &version); err != 0 {
		return 0, FDBError{Code: err}
	}
	return int64(version), nil
}

func (t *Transaction) Reset() {
	C.fdb_transaction_reset(t.t)
}

func boolToInt(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func (t *Transaction) GetKey(sel KeySelector) *FutureKey {
	k := &FutureKey{future: future{f: C.fdb_transaction_get_key(t.t, (*C.uint8_t)(unsafe.Pointer(&sel.Key[0])), C.int(len(sel.Key)), C.fdb_bool_t(boolToInt(sel.OrEqual)), C.int(sel.Offset), 0)}}
	runtime.SetFinalizer(k, (*FutureKey).destroy)
	return k
}
