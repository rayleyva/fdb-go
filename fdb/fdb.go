package fdb

/*
 #cgo LDFLAGS: -lfdb_c -lm
 #define FDB_API_VERSION 100
 #include <foundationdb/fdb_c.h>

 extern void notifyChannel(void*);

 void go_callback(FDBFuture* f, void* ch) {
     notifyChannel(ch);
 }

 void go_set_callback(void* f, void* ch) {
     fdb_future_set_callback(f, (FDBCallback)&go_callback, ch);
 }
 */
import "C"

import (
	"unsafe"
	"runtime"
	"fmt"
)

type FDBError struct {
	code C.fdb_error_t
}

func (e FDBError) Error() string {
	return fmt.Sprintf("%s (%d)", C.GoString(C.fdb_get_error(C.fdb_error_t(e.code))), e.code)
}

func Init() error {
	var e C.fdb_error_t
	if e = C.fdb_select_api_version_impl(23, 23); e != 0 {
		return fmt.Errorf("FoundationDB API error")
	}
	if e = C.fdb_setup_network(); e != 0 {
		return FDBError{code:e}
	}
	go C.fdb_run_network()
	return nil
}

type future struct {
	f *C.FDBFuture
}

func fdb_future_block_until_ready(f *C.FDBFuture) {
	if C.fdb_future_is_ready(f) != 0 {
		return
	}

	ch := make(chan struct{})
	C.go_set_callback(unsafe.Pointer(f), unsafe.Pointer(&ch))
	<-ch
}

func (f *future) BlockUntilReady() {
	fdb_future_block_until_ready(f.f)
}

func (f *future) IsReady() bool {
	return C.fdb_future_is_ready(f.f) != 0
}

type Cluster struct {
	c *C.FDBCluster
}

func (c *Cluster) destroy() {
	C.fdb_cluster_destroy(c.c)
}

func CreateCluster() (*Cluster, error) {
	f := C.fdb_create_cluster(nil)
	fdb_future_block_until_ready(f)
	outc := &C.FDBCluster{}
	if err := C.fdb_future_get_cluster(f, &outc); err != 0 {
		return nil, FDBError{code:err}
	}
	C.fdb_future_destroy(f)
	c := &Cluster{c: outc}
	runtime.SetFinalizer(c, (*Cluster).destroy)
	return c, nil
}

func (c *Cluster) OpenDatabase (dbname []byte) (*Database, error) {
	f := C.fdb_cluster_create_database(c.c, (*C.uint8_t)(unsafe.Pointer(&dbname[0])), C.int(len(dbname)))
	fdb_future_block_until_ready(f)
	outd := &C.FDBDatabase{}
	if err := C.fdb_future_get_database(f, &outd); err != 0 {
		return nil, FDBError{code:err}
	}
	C.fdb_future_destroy(f)
	d := &Database{d: outd}
	runtime.SetFinalizer(d, (*Database).destroy)
	return d, nil
}

type Database struct {
	d *C.FDBDatabase
}

func (d *Database) destroy() {
	println("database destroy")
	C.fdb_database_destroy(d.d)
}

func (d *Database) CreateTransaction () (*Transaction, error) {
	outt := &C.FDBTransaction{}
	if err := C.fdb_database_create_transaction(d.d, &outt); err != 0 {
		return nil, FDBError{code:err}
	}
	t := &Transaction{outt}
	runtime.SetFinalizer(t, (*Transaction).destroy)
	return t, nil
}

type Transaction struct {
	t *C.FDBTransaction
}

func (t *Transaction) destroy() {
	C.fdb_transaction_destroy(t.t)
}

type Value struct {
	future
	v []byte
	set bool
}

func (v *Value) destroy() {
	C.fdb_future_destroy(v.f)
}

func (v *Value) Get() ([]byte, error) {
	if v.set {
		return v.v, nil
	}

	v.BlockUntilReady()
	var present C.fdb_bool_t
	var value *C.uint8_t
	var length C.int
	if err := C.fdb_future_get_value(v.f, &present, &value, &length); err != 0 {
		if err != 2017 {
			return nil, FDBError{code:err}
		}
	}
	if present != 0 {
		v.v = C.GoBytes(unsafe.Pointer(value), length)
	}
	v.set = true
	C.fdb_future_release_memory(v.f)
	return v.v, nil
}

func (t *Transaction) Get(key []byte) *Value {
	v := &Value{future: future{f:C.fdb_transaction_get(t.t, (*C.uint8_t)(unsafe.Pointer(&key[0])), C.int(len(key)), 0)}}
	runtime.SetFinalizer(v, (*Value).destroy)
	return v
}

func (t *Transaction) Set(key []byte, value []byte) {
	C.fdb_transaction_set(t.t, (*C.uint8_t)(unsafe.Pointer(&key[0])), C.int(len(key)), (*C.uint8_t)(unsafe.Pointer(&value[0])), C.int(len(value)))
}

func (t *Transaction) Clear(key []byte) {
	C.fdb_transaction_clear(t.t, (*C.uint8_t)(unsafe.Pointer(&key[0])), C.int(len(key)))
}

type FutureNil struct {
	future
}

func makeFutureNil(f *C.FDBFuture) *FutureNil {
	ret := &FutureNil{future: future{f:f}}
	runtime.SetFinalizer(ret, (*FutureNil).destroy)
	return ret
}

func (f *FutureNil) destroy() {
	C.fdb_future_destroy(f.f)
}

func (f *FutureNil) Wait() error {
	fdb_future_block_until_ready(f.f)
	if err := C.fdb_future_get_error(f.f); err != 0 {
		return FDBError{code:err}
	}
	return nil
}

func (t *Transaction) Commit() *FutureNil {
	return makeFutureNil(C.fdb_transaction_commit(t.t))
}
