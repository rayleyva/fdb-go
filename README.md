fdb-go
======

[Go language](http://golang.org) bindings for [FoundationDB](https://foundationdb.com), a distributed key-value store with ACID transactions.

This package is currently targetting FoundationDB 1.0.0 (API version 100), and is a **work in progress**. In particular, there is not yet complete coverage of the FoundationDB API, and the interface will almost certainly change in breaking ways.

Example
-------

Until there's proper documentation, here's a quick code snippet.

    package main

    import (
    	"github.com/FoundationDB/fdb-go/fdb"
    	"log"
    	"fmt"
    	"sync"
    )

    func main() {
    	if err := fdb.Init(); err != nil {
    		log.Fatal(err)
    	}

    	cluster, err := fdb.CreateCluster()
    	if err != nil {
    		log.Fatal(err)
    	}

    	db, err := cluster.OpenDatabase([]byte("DB"))
    	if err != nil {
    		log.Fatal(err)
    	}

    	// db.Transact will create a new transaction and commit it for you,
    	// retrying the function as necessary. The calling goroutine will be
    	// blocked until success or non-retryable error.
    	//
    	// In this example, we are ignoring the possible return value of the
    	// transactional function.
    	_, err = db.Transact(func (tr *fdb.Transaction) (interface{}, error) {
    		tr.ClearRange([]byte("go"), []byte("gp"))
    		tr.Set([]byte("gofoo"), []byte("hello"))
    		tr.Set([]byte("gobar"), []byte("world"))
    		return nil, nil
    	})
    	if err != nil {
    		log.Fatal(err)
    	}

    	wg := sync.WaitGroup{}
    	ch := make(chan struct{})

    	wg.Add(1)
    	go func() {
    		ret, err := db.Transact(func (tr *fdb.Transaction) (interface{}, error) {
    			// tr.Get() is a "future" representing an asynchronous
    			// result, and does not block.
    			fv1 := tr.Get([]byte("gofoo"))
    			fv2 := tr.Get([]byte("gobar"))

    			// GetOrPanic() will block until the future is ready,
    			// and panic any FoundationDB errors, allowing
    			// db.Transact() to recover and (possibly) retry the
    			// transaction.
    			v1 := fv1.GetOrPanic()
    			// GetWithError() also blocks, but allows us to handle
    			// the error ourselves. It's still important that we
    			// check the error and return it if non-nil, allowing
    			// db.Transact() to react appropriately.
    			v2, e := fv2.GetWithError()
    			if e != nil {
    				return nil, e
    			}

    			fmt.Println("inside first goroutine\n  gofoo:", string(v1), "\n  gobar:", string(v2))

    			// Wait for the other goroutine to finish, which should
    			// trigger a conflict in this transaction.
    			<- ch

    			tr.Set([]byte("gofoo"), []byte("aloha"))

    			// We can return anything that implements interface{},
    			// along with an error. Here we are returning the
    			// last-seen values of gofoo and gobar before our own
    			// set.
    			return &struct{a, b string}{string(v1), string(v2)}, nil
    		})
    		if err != nil {
    			log.Fatal(err)
    		}

    		fmt.Println("first goroutine returned:", ret)
    		wg.Done()
    	}()

    	wg.Add(1)
    	go func() {
    		w, err := db.Transact(func (tr *fdb.Transaction) (interface{}, error) {
    			tr.Set([]byte("gofoo"), []byte("hola"))
    			return tr.Watch([]byte("gofoo")), nil
    		})
    		if err != nil {
    			log.Fatal(err)
    		}

    		fmt.Println("second goroutine modified gofoo")

    		// Let the first goroutine proceed.
    		close(ch)

    		// This watch should trigger once the first goroutine
    		// successfully commits a change to "gofoo".
    		err = w.(*fdb.FutureNil).GetWithError()
    		fmt.Println("watch triggered on gofoo")
    		wg.Done()
    	}()

    	wg.Wait()
    }
