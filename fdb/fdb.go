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
		return fmt.Errorf("FoundationDB API error (requested API 100)")
	}
	if e = C.fdb_setup_network(); e != 0 {
		return FDBError{Code: e}
	}
	go C.fdb_run_network()
	return nil
}
