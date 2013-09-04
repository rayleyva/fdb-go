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
	"runtime"
)

type ReadTransaction interface {
	Get(key []byte) *FutureValue
	GetKey(sel KeySelector) *FutureKey
	GetRange(begin []byte, end []byte, options RangeOptions) *RangeResult
	GetRangeSelector(begin KeySelector, end KeySelector, options RangeOptions) *RangeResult
	GetRangeStartsWith(prefix []byte, options RangeOptions) *RangeResult
	GetReadVersion() *FutureVersion
}

type Transaction struct {
	t *C.FDBTransaction
	Options TransactionOptions
}

type TransactionOptions struct {
	transaction *Transaction
}

func (opt TransactionOptions) setOpt(code int, param []byte) error {
	if opt.transaction == nil {
		return &Error{errorClientInvalidOperation}
	}

	return setOpt(func(p *C.uint8_t, pl C.int) C.fdb_error_t {
		return C.fdb_transaction_set_option(opt.transaction.t, C.FDBTransactionOption(code), p, pl)
	}, param)
}

func (t *Transaction) destroy() {
	C.fdb_transaction_destroy(t.t)
}

func (t *Transaction) Cancel() {
	if t.t != nil {
		C.fdb_transaction_cancel(t.t)
	}
}

func (t *Transaction) SetReadVersion(version int64) {
	if t.t != nil {
		C.fdb_transaction_set_read_version(t.t, C.int64_t(version))
	}
}

func (t *Transaction) Snapshot() *Snapshot {
	return &Snapshot{t}
}

func (t *Transaction) OnError(e *Error) *FutureNil {
	// FIXME: if t.t == nil we want to return a Future that will act like Error{errorClientInvalidOperation}
	return makeFutureNil(C.fdb_transaction_on_error(t.t, e.code))
}

func (t *Transaction) Commit() *FutureNil {
	// FIXME: if t.t == nil we want to return a Future that will act like Error{errorClientInvalidOperation}
	return makeFutureNil(C.fdb_transaction_commit(t.t))
}

func (t *Transaction) Watch(key []byte) *FutureNil {
	// FIXME: if t.t == nil we want to return a Future that will act like Error{errorClientInvalidOperation}
	return makeFutureNil(C.fdb_transaction_watch(t.t, byteSliceToPtr(key), C.int(len(key))))
}

func (t *Transaction) get(key []byte, snapshot int) *FutureValue {
	// FIXME: if t.t == nil we want to return a Future that will act like Error{errorClientInvalidOperation}
	v := &FutureValue{future: future{f: C.fdb_transaction_get(t.t, byteSliceToPtr(key), C.int(len(key)), C.fdb_bool_t(snapshot))}}
	runtime.SetFinalizer(v, (*FutureValue).destroy)
	return v
}

func (t *Transaction) Get(key []byte) *FutureValue {
	return t.get(key, 0)
}

func (t *Transaction) doGetRange(begin KeySelector, end KeySelector, options RangeOptions, snapshot bool, iteration int) *FutureKeyValueArray {
	// FIXME: if t.t == nil we want to return a Future that will act like Error{errorClientInvalidOperation}
	f := &FutureKeyValueArray{future: future{f: C.fdb_transaction_get_range(t.t, byteSliceToPtr(begin.Key), C.int(len(begin.Key)), C.fdb_bool_t(boolToInt(begin.OrEqual)), C.int(begin.Offset), byteSliceToPtr(end.Key), C.int(len(end.Key)), C.fdb_bool_t(boolToInt(end.OrEqual)), C.int(end.Offset), C.int(options.Limit), C.int(0), C.FDBStreamingMode(options.Mode-1), C.int(iteration), C.fdb_bool_t(boolToInt(snapshot)), C.fdb_bool_t(boolToInt(options.Reverse)))}}
	runtime.SetFinalizer(f, (*FutureKeyValueArray).destroy)
	return f
}

func (t *Transaction) getRangeSelector(begin KeySelector, end KeySelector, options RangeOptions, snapshot bool) *RangeResult {
	rr := RangeResult{
		t: t,
		begin: begin,
		end: end,
		options: options,
		snapshot: snapshot,
		f: t.doGetRange(begin, end, options, snapshot, 1),
	}
	return &rr
}

func (t *Transaction) GetRangeSelector(begin KeySelector, end KeySelector, options RangeOptions) *RangeResult {
	return t.getRangeSelector(begin, end, options, false)
}

func (t *Transaction) GetRange(begin []byte, end []byte, options RangeOptions) *RangeResult {
	return t.getRangeSelector(FirstGreaterOrEqual(begin), FirstGreaterOrEqual(end), options, false)
}

// FIXME: prefix is 0xFF*?
func strinc(prefix []byte) []byte {
	ret := make([]byte, len(prefix))
	copy(ret, prefix)
	for i := len(prefix); i > 0; i-- {
		if prefix[i-1] != 0xFF {
			ret[i-1] += 1
			return ret
		}
	}
	return prefix
}

func (t *Transaction) GetRangeStartsWith(prefix []byte, options RangeOptions) *RangeResult {
	return t.getRangeSelector(FirstGreaterOrEqual(prefix), FirstGreaterOrEqual(strinc(prefix)), options, false)
}

func (t *Transaction) GetReadVersion() *FutureVersion {
	// FIXME: if t.t == nil we want to return a Future that will act like Error{errorClientInvalidOperation}
	v := &FutureVersion{future: future{f: C.fdb_transaction_get_read_version(t.t)}}
	runtime.SetFinalizer(v, (*FutureVersion).destroy)
	return v
}

func (t *Transaction) Set(key []byte, value []byte) {
	if t.t != nil {
		C.fdb_transaction_set(t.t, byteSliceToPtr(key), C.int(len(key)), byteSliceToPtr(value), C.int(len(value)))
	}
}

func (t *Transaction) Clear(key []byte) {
	if t.t != nil {
		C.fdb_transaction_clear(t.t, byteSliceToPtr(key), C.int(len(key)))
	}
}

func (t *Transaction) ClearRange(begin []byte, end []byte) {
	if t.t != nil {
		C.fdb_transaction_clear_range(t.t, byteSliceToPtr(begin), C.int(len(begin)), byteSliceToPtr(end), C.int(len(end)))
	}
}

func (t *Transaction) ClearRangeStartsWith(prefix []byte) {
	if t.t != nil {
		C.fdb_transaction_clear_range(t.t, byteSliceToPtr(prefix), C.int(len(prefix)), byteSliceToPtr(strinc(prefix)), C.int(len(prefix)))
	}
}

func (t *Transaction) GetCommittedVersion() (int64, error) {
	if t.t == nil {
		return 0, &Error{errorClientInvalidOperation}
	}

	var version C.int64_t

	if err := C.fdb_transaction_get_committed_version(t.t, &version); err != 0 {
		return 0, &Error{err}
	}

	return int64(version), nil
}

func (t *Transaction) Reset() {
	if t.t != nil {
		C.fdb_transaction_reset(t.t)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func (t *Transaction) getKey(sel KeySelector, snapshot int) *FutureKey {
	// FIXME: if t.t == nil we want to return a Future that will act like Error{errorClientInvalidOperation}
	k := &FutureKey{future: future{f: C.fdb_transaction_get_key(t.t, byteSliceToPtr(sel.Key), C.int(len(sel.Key)), C.fdb_bool_t(boolToInt(sel.OrEqual)), C.int(sel.Offset), C.fdb_bool_t(snapshot))}}
	runtime.SetFinalizer(k, (*FutureKey).destroy)
	return k
}

func (t *Transaction) GetKey(sel KeySelector) *FutureKey {
	return t.getKey(sel, 0)
}

func (t *Transaction) atomicOp(key []byte, param []byte, code int) {
	if t.t != nil {
		C.fdb_transaction_atomic_op(t.t, byteSliceToPtr(key), C.int(len(key)), byteSliceToPtr(param), C.int(len(param)), C.FDBMutationType(code))
	}
}

func (t *Transaction) addConflictRange(begin []byte, end []byte, crtype conflictRangeType) error {
	if t.t == nil {
		return &Error{errorClientInvalidOperation}
	}

	if err := C.fdb_transaction_add_conflict_range(t.t, byteSliceToPtr(begin), C.int(len(begin)), byteSliceToPtr(end), C.int(len(end)), C.FDBConflictRangeType(crtype)); err != 0 {
		return &Error{err}
	}

	return nil
}

func (t *Transaction) AddReadConflictRange(begin []byte, end []byte) error {
	return t.addConflictRange(begin, end, conflictRangeTypeRead)
}

func (t *Transaction) AddReadConflictKey(key []byte) error {
	return t.addConflictRange(key, append(key, 0x00), conflictRangeTypeRead)
}

func (t *Transaction) AddWriteConflictRange(begin []byte, end []byte) error {
	return t.addConflictRange(begin, end, conflictRangeTypeWrite)
}

func (t *Transaction) AddWriteConflictKey(key []byte) error {
	return t.addConflictRange(key, append(key, 0x00), conflictRangeTypeWrite)
}

type Snapshot struct {
	t *Transaction
}

func (s *Snapshot) Get(key []byte) *FutureValue {
	// FIXME: something should be checked, surely?
	return s.t.get(key, 1)
}

func (s *Snapshot) GetKey(sel KeySelector) *FutureKey {
	// FIXME: something should be checked, surely?
	return s.t.getKey(sel, 1)
}

func (s *Snapshot) GetRangeSelector(begin KeySelector, end KeySelector, options RangeOptions) *RangeResult {
	// FIXME: something should be checked, surely?
	return s.t.getRangeSelector(begin, end, options, true)
}

func (s *Snapshot) GetRange(begin []byte, end []byte, options RangeOptions) *RangeResult {
	// FIXME: something should be checked, surely?
	return s.t.getRangeSelector(FirstGreaterOrEqual(begin), FirstGreaterOrEqual(end), options, true)
}

func (s *Snapshot) GetRangeStartsWith(prefix []byte, options RangeOptions) *RangeResult {
	// FIXME: something should be checked, surely?
	return s.t.getRangeSelector(FirstGreaterOrEqual(prefix), FirstGreaterOrEqual(strinc(prefix)), options, true)
}

func (s *Snapshot) GetReadVersion() *FutureVersion {
	// FIXME: something should be checked, surely?
	return s.t.GetReadVersion()
}
