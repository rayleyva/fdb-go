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
	"runtime"
	"sync"
	"unsafe"
)

/* Would put this in futures.go but for the documented issue with
/* exports and functions in preamble
/* (https://code.google.com/p/go-wiki/wiki/cgo#Global_functions) */
//export notifyChannel
func notifyChannel(ch *chan struct{}) {
	*ch <- struct{}{}
}

// Error represents a low-level error returned by the FoundationDB C library. An
// Error may be returned by any FoundationDB API function that returns error, or
// as a panic from any FoundationDB API function whose name ends with OrPanic.
//
// An Error may be cast to an int for comparison against the list of
// FoundationDB error codes at
// https://foundationdb.com/documentation/api-error-codes.html, but generally
// should be passed to (Transaction).OnError. When using (Database).Transact,
// non-fatal errors will be retried automatically.
type Error C.fdb_error_t

func (e Error) Error() string {
	return fmt.Sprintf("FDB Error: %s (%d)", C.GoString(C.fdb_get_error(C.fdb_error_t(e))), e)
}

// A Transactor represents an object that can execute a transactional
// function. Functions that accept a Transactor can be called with either a
// Database or a Transaction, allowing them to be composed transactionally.
type Transactor interface {
	Transact(func (tr Transaction) (interface{}, error)) (interface{}, error)
}

func setOpt(setter func(*C.uint8_t, C.int) C.fdb_error_t, param []byte) error {
	if err := setter(byteSliceToPtr(param), C.int(len(param))); err != 0 {
		return Error(err)
	}

	return nil
}

// NetworkOptions is a handle with which to set options that affect the entire
// FoundationDB client. A NetworkOptions instance should be obtained with the
// fdb.Options() method.
type NetworkOptions struct {
}

// Options returns a NetworkOptions instance suitable for setting options that
// affect the entire FoundationDB client.
func Options() NetworkOptions {
	return NetworkOptions{}
}

func (opt NetworkOptions) setOpt(code int, param []byte) error {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return errorApiVersionUnset
	}

	return setOpt(func(p *C.uint8_t, pl C.int) C.fdb_error_t {
		return C.fdb_network_set_option(C.FDBNetworkOption(code), p, pl)
	}, param)
}

// APIVersion determines the runtime behavior the fdb package. If the requested
// version is not supported by both the fdb package and the FoundationDB C
// library, an error will be returned. APIVersion must be called prior to any
// other functions in the fdb package.
//
// Currently, the only API versions 100 and 101 are supported.
func APIVersion(version int) error {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion != 0 {
		return errorApiVersionAlreadySet
	}

	if version < 100 || version > 101 {
		return errorApiVersionNotSupported
	}

	if e := C.fdb_select_api_version_impl(C.int(version), 101); e != 0 {
		return Error(e)
	}

	apiVersion = version

	return nil
}

var apiVersion int
var networkStarted bool
var networkMutex sync.Mutex

var openClusters map[string]Cluster
var openDatabases map[string]Database

func init() {
	openClusters = make(map[string]Cluster)
	openDatabases = make(map[string]Database)
}

func startNetwork() error {
	if e := C.fdb_setup_network(); e != 0 {
		return Error(e)
	}

	go C.fdb_run_network()

	networkStarted = true

	return nil
}

// StartNetwork initializes the FoundationDB client networking engine. It is not
// necessary to call StartNetwork when using the fdb.Open() or fdb.OpenDefault()
// functions to obtain a database handle. StartNetwork must not be called more
// than once.
func StartNetwork() error {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return errorApiVersionUnset
	}

	return startNetwork()
}

// DefaultClusterFile should be passed to fdb.Open() or fdb.CreateCluster() to
// allow the FoundationDB C library to select the platform-appropriate default
// cluster file on the current machine.
const DefaultClusterFile string = ""

// OpenDefault returns a database handle to the default database from the
// FoundationDB cluster identified by the DefaultClusterFile on the current
// machine. The FoundationDB client networking engine will be initialized first,
// if necessary.
func OpenDefault() (Database, error) {
	return Open(DefaultClusterFile, []byte("DB"))
}

// Open returns a database handle to the named database from the FoundationDB
// cluster identified by the provided cluster file and database name. The
// FoundationDB client networking engine will be initialized first, if
// necessary.
//
// In the current release, the database name must be []byte("DB").
func Open(clusterFile string, dbName []byte) (Database, error) {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return Database{}, errorApiVersionUnset
	}

	var e error

	if !networkStarted {
		e = startNetwork()
		if e != nil {
			return Database{}, e
		}
	}

	cluster, ok := openClusters[clusterFile]
	if !ok {
		cluster, e = createCluster(clusterFile)
		if e != nil {
			return Database{}, e
		}
		openClusters[clusterFile] = cluster
	}

	db, ok := openDatabases[string(dbName)]
	if !ok {
		db, e = cluster.OpenDatabase(dbName)
		if e != nil {
			return Database{}, e
		}
		openDatabases[string(dbName)] = db
	}

	return db, nil
}

func createCluster(clusterFile string) (Cluster, error) {
	var cf *C.char

	if len(clusterFile) != 0 {
		cf = C.CString(clusterFile)
	}

	f := C.fdb_create_cluster(cf)
	fdb_future_block_until_ready(f)

	var outc *C.FDBCluster

	if err := C.fdb_future_get_cluster(f, &outc); err != 0 {
		return Cluster{}, Error(err)
	}

	C.fdb_future_destroy(f)

	c := &cluster{outc}
	runtime.SetFinalizer(c, (*cluster).destroy)

	return Cluster{c}, nil
}

// CreateCluster returns a cluster handle to the FoundationDB cluster identified
// by the provided cluster file.
func CreateCluster(clusterFile string) (Cluster, error) {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return Cluster{}, errorApiVersionUnset
	}

	if !networkStarted {
		return Cluster{}, errorNetworkNotSetup
	}

	return createCluster(clusterFile)
}

func byteSliceToPtr(b []byte) *C.uint8_t {
	if len(b) > 0 {
		return (*C.uint8_t)(unsafe.Pointer(&b[0]))
	} else {
		return nil
	}
}

// KeyConvertible is the interface implemented by types which may be used as
// FoundationDB Keys. The fdb.Key type satisfies the KeyConvertible interface.
type KeyConvertible interface {
	ToFDBKey() Key
}

// Key represents a FoundationDB key, a lexicographically-ordered sequence of
// bytes. Key implements the KeyConvertible and Selectable interfaces.
type Key []byte

// ToFDBKey allows Key to (trivially) satisfy the KeyConvertible interface.
func (k Key) ToFDBKey() Key {
	return k
}

// ToFDBKeySelector allows Key to satisfy the Selectable interface. The returned
// selector describes the first key in the database lexicographically greater
// than or equal to k.
func (k Key) ToFDBKeySelector() KeySelector {
	return FirstGreaterOrEqual(k)
}
