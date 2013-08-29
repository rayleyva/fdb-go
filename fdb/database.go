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

type Database struct {
	d *C.FDBDatabase
	Options databaseOptions
}

type databaseOptions struct {
	database *Database
}

func (opt databaseOptions) setOpt(code int, param []byte, paramLen int) error {
	return setOpt(func(p *C.uint8_t, pl C.int) C.fdb_error_t {
		return C.fdb_database_set_option(opt.database.d, C.FDBDatabaseOption(code), p, pl)
	}, param, paramLen)
}

func (d *Database) destroy() {
	C.fdb_database_destroy(d.d)
}

func (d *Database) CreateTransaction() (*Transaction, error) {
	outt := &C.FDBTransaction{}
	if err := C.fdb_database_create_transaction(d.d, &outt); err != 0 {
		return nil, Error{Code: err}
	}
	t := &Transaction{t: outt}
	t.Options.transaction = t
	runtime.SetFinalizer(t, (*Transaction).destroy)
	return t, nil
}

func (d *Database) Transact(f func(tr *Transaction) (interface{}, error)) (ret interface{}, e error) {
	tr, e := d.CreateTransaction()
	/* Any error here is non-retryable */
	if e != nil {
		return
	}

	wrapped := func() {
		defer func() {
			if r := recover(); r != nil {
				switch r := r.(type) {
				case Error:
					e = r
				default:
					panic(r)
				}
			}
		}()

		ret, e = f(tr)

		if e != nil {
			return
		}

		e = tr.Commit().GetWithError()
	}

	for {
		wrapped()

		/* No error means success! */
		if e == nil {
			return
		}

		switch ep := e.(type) {
		case Error:
			e = tr.OnError(ep).GetWithError()
		}

		/* If OnError returns an error, then it's not
		/* retryable; otherwise take another pass at things */
		if e != nil {
			return
		}
	}
}
