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

// A ReadTransaction represents an object that can asynchronously read from a
// FoundationDB database. Transaction and Snapshot both satisfy the
// ReadTransaction interface.
type ReadTransaction interface {
	Get(key []byte) FutureValue
	GetKey(sel KeySelector) FutureKey
	GetRange(begin []byte, end []byte, options RangeOptions) RangeResult
	GetRangeSelector(begin KeySelector, end KeySelector, options RangeOptions) RangeResult
	GetReadVersion() FutureVersion
}

// Transaction is a handle to a FoundationDB transaction. Transaction is a
// lightweight object that may be efficiently copied, and is safe for concurrent
// use by multiple goroutines.
//
// In FoundationDB, a transaction is a mutable snapshot of a database. All read
// and write operations on a transaction see and modify an otherwise-unchanging
// version of the database and only change the underlying database if and when
// the transaction is committed. Read operations do see the effects of previous
// write operations on the same transaction. Committing a transaction usually
// succeeds in the absence of conflicts.
//
// Transactions group operations into a unit with the properties of atomicity,
// isolation, and durability. Transactions also provide the ability to maintain
// an application’s invariants or integrity constraints, supporting the property
// of consistency. Together these properties are known as ACID.
//
// Transactions are also causally consistent: once a transaction has been
// successfully committed, all subsequently created transactions will see the
// modifications made by it.
type Transaction struct {
	*transaction
}

type transaction struct {
	ptr *C.FDBTransaction
}

// TransactionOptions is a handle with which to set options that affect a
// Transaction object. A TransactionOptions instance should be obtained with the
// (Transaction).Options method.
type TransactionOptions struct {
	transaction *transaction
}

func (opt TransactionOptions) setOpt(code int, param []byte) error {
	return setOpt(func(p *C.uint8_t, pl C.int) C.fdb_error_t {
		return C.fdb_transaction_set_option(opt.transaction.ptr, C.FDBTransactionOption(code), p, pl)
	}, param)
}

func (t *transaction) destroy() {
	C.fdb_transaction_destroy(t.ptr)
}

// Transact passes the Transaction receiver object to the caller-provided
// function, but does not handle errors or commit the transaction.
//
// Transact makes Transaction satisfy the Transactor interface, allowing
// transactional functions to be used compositionally.
func (t Transaction) Transact(f func (tr Transaction) (interface{}, error)) (interface{}, error) {
	return f(t)
}

// Cancel cancels a transaction. All pending or future uses of the transaction
// will encounter an error. The Transaction object may be reused after calling
// (Transaction).Reset.
//
// Be careful if you are using (Transaction).Reset and (Transaction).Cancel
// concurrently with the same transaction. Since they negate each other’s
// effects, a race condition between these calls will leave the transaction in
// an unknown state.
//
// If your program attempts to cancel a transaction after (Transaction).Commit
// has been called but before it returns, unpredictable behavior will
// result. While it is guaranteed that the transaction will eventually end up in
// a cancelled state, the commit may or may not occur. Moreover, even if the
// call to (Transaction).Commit appears to return a transaction_cancelled error,
// the commit may have occurred or may occur in the future. This can make it
// more difficult to reason about the order in which transactions occur.
func (t Transaction) Cancel() {
	C.fdb_transaction_cancel(t.ptr)
}

// (Infrequently used) SetReadVersion sets the database version that the
// transaction will read from the database. The database cannot guarantee causal
// consistency if this method is used (the transaction’s reads will be causally
// consistent only if the provided read version has that property).
func (t Transaction) SetReadVersion(version int64) {
	C.fdb_transaction_set_read_version(t.ptr, C.int64_t(version))
}

// Snapshot returns a Snapshot object, suitable for performing snapshot
// reads. Snapshot reads offer a more relaxed isolation level than
// FoundationDB's default serializable isolation, reducing transaction conflicts
// but making it harder to reason about concurrency.
//
// For more information on snapshot reads, see
// https://foundationdb.com/documentation/developer-guide.html#using-snapshot-reads.
func (t Transaction) Snapshot() Snapshot {
	return Snapshot{t.transaction}
}

func makeFutureNil(fp *C.FDBFuture) FutureNil {
	f := &future{fp}
	runtime.SetFinalizer(f, (*future).destroy)
	return FutureNil{f}
}

// OnError determines whether an fdb.Error instance represents a retryable or
// fatal error. Waiting on the returned future will return the same error when
// fatal, or return nil (after blocking the calling goroutine for a suitable
// delay) for retryable errors.
//
// Typical code will not use OnError directly. (Database).Transact uses OnError
// internally to implement a retry loop.
func (t Transaction) OnError(e Error) FutureNil {
	return makeFutureNil(C.fdb_transaction_on_error(t.ptr, C.fdb_error_t(e)))
}

func (t Transaction) Commit() FutureNil {
	return makeFutureNil(C.fdb_transaction_commit(t.ptr))
}

func (t Transaction) Watch(key []byte) FutureNil {
	return makeFutureNil(C.fdb_transaction_watch(t.ptr, byteSliceToPtr(key), C.int(len(key))))
}

func (t *transaction) get(key []byte, snapshot int) FutureValue {
	f := &future{C.fdb_transaction_get(t.ptr, byteSliceToPtr(key), C.int(len(key)), C.fdb_bool_t(snapshot))}
	runtime.SetFinalizer(f, (*future).destroy)
	return FutureValue{&futureValue{future: f}}
}

func (t Transaction) Get(key []byte) FutureValue {
	return t.get(key, 0)
}

func (t *transaction) doGetRange(begin KeySelector, end KeySelector, options RangeOptions, snapshot bool, iteration int) futureKeyValueArray {
	f := &future{C.fdb_transaction_get_range(t.ptr, byteSliceToPtr(begin.Key), C.int(len(begin.Key)), C.fdb_bool_t(boolToInt(begin.OrEqual)), C.int(begin.Offset), byteSliceToPtr(end.Key), C.int(len(end.Key)), C.fdb_bool_t(boolToInt(end.OrEqual)), C.int(end.Offset), C.int(options.Limit), C.int(0), C.FDBStreamingMode(options.Mode-1), C.int(iteration), C.fdb_bool_t(boolToInt(snapshot)), C.fdb_bool_t(boolToInt(options.Reverse)))}
	runtime.SetFinalizer(f, (*future).destroy)
	return futureKeyValueArray{f}
}

func (t *transaction) getRangeSelector(begin KeySelector, end KeySelector, options RangeOptions, snapshot bool) RangeResult {
	f := t.doGetRange(begin, end, options, snapshot, 1)
	return RangeResult{
		t: t,
		begin: begin,
		end: end,
		options: options,
		snapshot: snapshot,
		f: &f,
	}
}

func (t Transaction) GetRangeSelector(begin KeySelector, end KeySelector, options RangeOptions) RangeResult {
	return t.getRangeSelector(begin, end, options, false)
}

func (t Transaction) GetRange(begin []byte, end []byte, options RangeOptions) RangeResult {
	return t.getRangeSelector(FirstGreaterOrEqual(begin), FirstGreaterOrEqual(end), options, false)
}

func (t *transaction) getReadVersion() FutureVersion {
	f := &future{C.fdb_transaction_get_read_version(t.ptr)}
	runtime.SetFinalizer(f, (*future).destroy)
	return FutureVersion{f}
}

func (t Transaction) GetReadVersion() FutureVersion {
	return t.getReadVersion()
}

func (t Transaction) Set(key []byte, value []byte) {
	C.fdb_transaction_set(t.ptr, byteSliceToPtr(key), C.int(len(key)), byteSliceToPtr(value), C.int(len(value)))
}

func (t Transaction) Clear(key []byte) {
	C.fdb_transaction_clear(t.ptr, byteSliceToPtr(key), C.int(len(key)))
}

func (t Transaction) ClearRange(begin []byte, end []byte) {
	C.fdb_transaction_clear_range(t.ptr, byteSliceToPtr(begin), C.int(len(begin)), byteSliceToPtr(end), C.int(len(end)))
}

func (t Transaction) GetCommittedVersion() (int64, error) {
	var version C.int64_t

	if err := C.fdb_transaction_get_committed_version(t.ptr, &version); err != 0 {
		return 0, Error(err)
	}

	return int64(version), nil
}

func (t Transaction) Reset() {
	C.fdb_transaction_reset(t.ptr)
}

func boolToInt(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func (t *transaction) getKey(sel KeySelector, snapshot int) FutureKey {
	f := &future{C.fdb_transaction_get_key(t.ptr, byteSliceToPtr(sel.Key), C.int(len(sel.Key)), C.fdb_bool_t(boolToInt(sel.OrEqual)), C.int(sel.Offset), C.fdb_bool_t(snapshot))}
	runtime.SetFinalizer(f, (*future).destroy)
	return FutureKey{&futureKey{future: f}}
}

func (t Transaction) GetKey(sel KeySelector) FutureKey {
	return t.getKey(sel, 0)
}

func (t Transaction) atomicOp(key []byte, param []byte, code int) {
	C.fdb_transaction_atomic_op(t.ptr, byteSliceToPtr(key), C.int(len(key)), byteSliceToPtr(param), C.int(len(param)), C.FDBMutationType(code))
}

func addConflictRange(t *transaction, begin []byte, end []byte, crtype conflictRangeType) error {
	if err := C.fdb_transaction_add_conflict_range(t.ptr, byteSliceToPtr(begin), C.int(len(begin)), byteSliceToPtr(end), C.int(len(end)), C.FDBConflictRangeType(crtype)); err != 0 {
		return Error(err)
	}

	return nil
}

func (t Transaction) AddReadConflictRange(begin []byte, end []byte) error {
	return addConflictRange(t.transaction, begin, end, conflictRangeTypeRead)
}

func (t Transaction) AddReadConflictKey(key []byte) error {
	return addConflictRange(t.transaction, key, append(key, 0x00), conflictRangeTypeRead)
}

func (t Transaction) AddWriteConflictRange(begin []byte, end []byte) error {
	return addConflictRange(t.transaction, begin, end, conflictRangeTypeWrite)
}

func (t Transaction) AddWriteConflictKey(key []byte) error {
	return addConflictRange(t.transaction, key, append(key, 0x00), conflictRangeTypeWrite)
}

func (t Transaction) Options() TransactionOptions {
	return TransactionOptions{t.transaction}
}

type Snapshot struct {
	*transaction
}

func (s Snapshot) Get(key []byte) FutureValue {
	return s.get(key, 1)
}

func (s Snapshot) GetKey(sel KeySelector) FutureKey {
	return s.getKey(sel, 1)
}

func (s Snapshot) GetRangeSelector(begin KeySelector, end KeySelector, options RangeOptions) RangeResult {
	return s.getRangeSelector(begin, end, options, true)
}

func (s Snapshot) GetRange(begin []byte, end []byte, options RangeOptions) RangeResult {
	return s.getRangeSelector(FirstGreaterOrEqual(begin), FirstGreaterOrEqual(end), options, true)
}

func (s Snapshot) GetReadVersion() FutureVersion {
	return s.getReadVersion()
}
