package fdb

/*
 #define FDB_API_VERSION 100
 #include <foundationdb/fdb_c.h>
*/
import "C"

import (
	"runtime"
	"unsafe"
)

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
		return nil, FDBError{Code: err}
	}
	C.fdb_future_destroy(f)
	c := &Cluster{c: outc}
	runtime.SetFinalizer(c, (*Cluster).destroy)
	return c, nil
}

func (c *Cluster) OpenDatabase(dbname []byte) (*Database, error) {
	f := C.fdb_cluster_create_database(c.c, (*C.uint8_t)(unsafe.Pointer(&dbname[0])), C.int(len(dbname)))
	fdb_future_block_until_ready(f)
	outd := &C.FDBDatabase{}
	if err := C.fdb_future_get_database(f, &outd); err != 0 {
		return nil, FDBError{Code: err}
	}
	C.fdb_future_destroy(f)
	d := &Database{d: outd}
	runtime.SetFinalizer(d, (*Database).destroy)
	return d, nil
}
