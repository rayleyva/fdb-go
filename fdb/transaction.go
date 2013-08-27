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
