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
}

func (d *Database) destroy() {
	C.fdb_database_destroy(d.d)
}

func (d *Database) CreateTransaction() (*Transaction, error) {
	outt := &C.FDBTransaction{}
	if err := C.fdb_database_create_transaction(d.d, &outt); err != 0 {
		return nil, FDBError{Code: err}
	}
	t := &Transaction{outt}
	runtime.SetFinalizer(t, (*Transaction).destroy)
	return t, nil
}

func (d *Database) Transact(f func(tr *Transaction) (interface{}, error)) (ret interface{}, e error) {
	tr, e := d.CreateTransaction()
	/* Any error here is non-retryable */
	if e != nil {
		return
	}

	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					fdberror, ok := r.(FDBError)
					if ok {
						e = fdberror
					} else {
						panic(r)
					}
				}
			}()

			ret, e = f(tr)

			if e != nil {
				return
			}

			e = tr.Commit().GetWithError()
		}()

		/* No error means success! */
		if e == nil {
			return
		}

		fdberr, ok := e.(FDBError)
		if ok {
			e = tr.OnError(fdberr).GetWithError()
		}

		/* If OnError returns an error, then it's not
		/* retryable; otherwise take another pass at things */
		if e != nil {
			return
		}
	}
}
