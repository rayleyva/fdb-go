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
 #cgo LDFLAGS: -lfdb_c -lm
 #define FDB_API_VERSION 100
 #include <foundationdb/fdb_c.h>
 #include <string.h>

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
	"runtime"
	"unsafe"
)

type future struct {
	f *C.FDBFuture
}

func fdb_future_block_until_ready(f *C.FDBFuture) {
	if C.fdb_future_is_ready(f) != 0 {
		return
	}

	ch := make(chan struct{}, 1)
	C.go_set_callback(unsafe.Pointer(f), unsafe.Pointer(&ch))
	<-ch
}

// BlockUntilReady blocks the calling goroutine until the future is
// ready. A future becomes ready either when it receives a value of
// its enclosed type (if any) or is set to an error state.
func (future *future) BlockUntilReady() {
	if future.f != nil {
		fdb_future_block_until_ready(future.f)
	}
}

// IsReady returns true if the future is ready, and false otherwise,
// without blocking. A future is ready either when has received a
// value of its enclosed type (if any) or has been set to an error
// state.
func (future *future) IsReady() bool {
	if future.f != nil {
		return C.fdb_future_is_ready(future.f) != 0
	}
	return true
}

// Cancel cancels a future and its associated asynchronous
// operation. If called before the future becomes ready, attempts to
// access the future will return an error. Cancel has no effect if the
// future is already ready.
//
// Note that even if a future is not ready, the associated
// asynchronous operation may already have completed and be unable to
// be cancelled.
func (future *future) Cancel() {
	if future.f != nil {
		C.fdb_future_cancel(future.f)
	}
}

type FutureValue struct {
	future
	v   []byte
	set bool
}

func (v *FutureValue) destroy() {
	C.fdb_future_destroy(v.f)
}

func (v *FutureValue) GetWithError() ([]byte, error) {
	if v.f == nil {
		return nil, &Error{errorClientInvalidOperation}
	}

	if v.set {
		return v.v, nil
	}

	fdb_future_block_until_ready(v.f)

	var present C.fdb_bool_t
	var value *C.uint8_t
	var length C.int
	if err := C.fdb_future_get_value(v.f, &present, &value, &length); err != 0 {
		if err != 2017 {
			return nil, &Error{err}
		}
	}
	if present != 0 {
		v.v = C.GoBytes(unsafe.Pointer(value), length)
	}
	v.set = true
	C.fdb_future_release_memory(v.f)
	return v.v, nil
}

func (v *FutureValue) GetOrPanic() []byte {
	val, err := v.GetWithError()
	if err != nil {
		panic(err)
	}
	return val
}

type FutureKey struct {
	future
	k []byte
}

func (k *FutureKey) destroy() {
	C.fdb_future_destroy(k.f)
}

func (k *FutureKey) GetWithError() ([]byte, error) {
	if k.f == nil {
		return nil, &Error{errorClientInvalidOperation}
	}

	if k.k != nil {
		return k.k, nil
	}

	fdb_future_block_until_ready(k.f)

	var value *C.uint8_t
	var length C.int
	if err := C.fdb_future_get_key(k.f, &value, &length); err != 0 {
		if err != 2017 {
			return nil, &Error{err}
		}
	}
	k.k = C.GoBytes(unsafe.Pointer(value), length)
	C.fdb_future_release_memory(k.f)
	return k.k, nil
}

func (k *FutureKey) GetOrPanic() []byte {
	val, err := k.GetWithError()
	if err != nil {
		panic(err)
	}
	return val
}

type FutureNil struct {
	future
}

func makeFutureNil(f *C.FDBFuture) *FutureNil {
	ret := &FutureNil{future: future{f: f}}
	runtime.SetFinalizer(ret, (*FutureNil).destroy)
	return ret
}

func (f *FutureNil) destroy() {
	C.fdb_future_destroy(f.f)
}

func (f *FutureNil) GetWithError() error {
	if f.f == nil {
		return &Error{errorClientInvalidOperation}
	}

	fdb_future_block_until_ready(f.f)
	if err := C.fdb_future_get_error(f.f); err != 0 {
		return &Error{err}
	}

	return nil
}

func (f *FutureNil) GetOrPanic() {
	if err := f.GetWithError(); err != nil {
		panic(err)
	}
}

type futureKeyValueArray struct {
	future
}

func (f *futureKeyValueArray) destroy() {
	C.fdb_future_destroy(f.f)
}

func stringRefToSlice(ptr uintptr) []byte {
	size := int(*((*C.int)(unsafe.Pointer(ptr+8))))

	if size == 0 {
		return []byte{}
	}

	ret := make([]byte, size)

	dst := unsafe.Pointer(&(ret[0]))
	src := unsafe.Pointer(*(**C.uint8_t)(unsafe.Pointer(ptr)))

	C.memcpy(dst, src, C.size_t(size))

	return ret
}

func (f *futureKeyValueArray) GetWithError() ([]KeyValue, bool, error) {
	if f.f == nil {
		return nil, false, &Error{errorClientInvalidOperation}
	}

	fdb_future_block_until_ready(f.f)

	var kvs *C.void
	var count C.int
	var more C.fdb_bool_t

	if err := C.fdb_future_get_keyvalue_array(f.f, (**C.FDBKeyValue)(unsafe.Pointer(&kvs)), &count, &more); err != 0 {
		return nil, false, &Error{err}
	}

	ret := make([]KeyValue, int(count))

	for i := 0; i < int(count); i++ {
		kvptr := uintptr(unsafe.Pointer(kvs)) + uintptr(i * 24)

		ret[i].Key = stringRefToSlice(kvptr)
		ret[i].Value = stringRefToSlice(kvptr + 12)

	}

 	return ret, (more != 0), nil
}

type FutureVersion struct {
	future
}

func (v *FutureVersion) destroy() {
	C.fdb_future_destroy(v.f)
}

func (v *FutureVersion) GetWithError() (int64, error) {
	if v.f == nil {
		return 0, &Error{errorClientInvalidOperation}
	}

	fdb_future_block_until_ready(v.f)

	var ver C.int64_t
	if err := C.fdb_future_get_version(v.f, &ver); err != 0 {
		return 0, &Error{err}
	}
	return int64(ver), nil
}

func (v *FutureVersion) GetOrPanic() int64 {
	val, err := v.GetWithError()
	if err != nil {
		panic(err)
	}
	return val
}
