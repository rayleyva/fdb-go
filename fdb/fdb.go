package fdb

/*
 #define FDB_API_VERSION 100
 #include <foundationdb/fdb_c.h>
*/
import "C"

import (
	"fmt"
)

/* Would put this in futures.go but for the documented issue with
/* exports and functions in preamble
/* (https://code.google.com/p/go-wiki/wiki/cgo#Global_functions) */
//export notifyChannel
func notifyChannel(ch *chan struct{}) {
	*ch <- struct{}{}
}

type FDBError struct {
	Code C.fdb_error_t
}

func (e FDBError) Error() string {
	return fmt.Sprintf("%s (%d)", C.GoString(C.fdb_get_error(e.Code)), e.Code)
}

func Init() error {
	var e C.fdb_error_t
	if e = C.fdb_select_api_version_impl(100, 100); e != 0 {
		return fmt.Errorf("FoundationDB API error")
	}
	if e = C.fdb_setup_network(); e != 0 {
		return FDBError{Code: e}
	}
	go C.fdb_run_network()
	return nil
}
