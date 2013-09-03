package main

import (
	"bitbucket.org/FoundationDB/fdb-go/fdb"
	"bitbucket.org/FoundationDB/fdb-go/fdb/tuple"
	"log"
	"fmt"
	"os"
	"strings"
	"sync"
	"runtime"
)

const verbose bool = false

func int64ToBool(i int64) bool {
	switch i {
	case 0:
		return false
	default:
		return true
	}
}

type StackMachine struct {
	prefix []byte
	tr *fdb.Transaction
	stack []interface{}
	lastVersion int64
	threads sync.WaitGroup
	verbose bool
	atomics map[string]func([]byte, []byte)
}

func newStackMachine(prefix []byte, verbose bool) *StackMachine {
	sm := StackMachine{verbose: verbose, prefix: prefix, atomics: make(map[string]func([]byte, []byte))}
	return &sm
}

func (sm *StackMachine) waitAndPop() (ret interface{}) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case fdb.Error:
				p, e := tuple.Pack(tuple.Tuple{[]byte("ERROR"), []byte(fmt.Sprintf("%d", int(r.Code)))})
				if e != nil {
					panic(e)
				}
				ret = p
			default:
				panic(r)
			}
		}
	}()

	var el interface{}
	el, sm.stack = sm.stack[len(sm.stack) - 1], sm.stack[:len(sm.stack) - 1]
	switch el := el.(type) {
	case int64, []byte, string:
		return el
	case *fdb.FutureNil:
		el.GetOrPanic()
		return []byte("RESULT_NOT_PRESENT")
	case *fdb.FutureValue:
		v := el.GetOrPanic()
		if v != nil {
			return v
		} else {
			return []byte("RESULT_NOT_PRESENT")
		}
	case *fdb.FutureKey:
		return el.GetOrPanic()
	case nil:
		return nil
	}
	log.Fatalf("Failed with %v %T\n", el, el)
	return nil
}

func (sm *StackMachine) pushRange(sl []fdb.KeyValue) {
	var t tuple.Tuple = make(tuple.Tuple, 0, len(sl) * 2)

	for _, kv := range(sl) {
		t = append(t, kv.Key)
		t = append(t, kv.Value)
	}

	p, e := tuple.Pack(t)
	if e != nil {
		panic(e)
	}

	sm.stack = append(sm.stack, p)
}

func (sm *StackMachine) store(item interface{}) {
	sm.stack = append(sm.stack, item)
}

func (sm *StackMachine) dumpStack() {
	for i := len(sm.stack) - 1; i >= 0; i-- {
		el := sm.stack[i]
		switch el := el.(type) {
		case int64:
			fmt.Printf(" %d", el)
		case *fdb.FutureNil:
			fmt.Printf(" FutureNil")
		case *fdb.FutureValue:
			fmt.Printf(" FutureValue")
		case *fdb.FutureKey:
			fmt.Printf(" FutureKey")
		case []byte:
			fmt.Printf(" %s", string(el))
		case string:
			fmt.Printf(" %s", el)
		case nil:
			fmt.Printf(" nil")
		default:
			log.Fatalf("Failed with %v %T\n", el, el)
		}
	}
}

func (sm *StackMachine) processInst(inst tuple.Tuple) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case fdb.Error:
				p, e := tuple.Pack(tuple.Tuple{[]byte("ERROR"), []byte(fmt.Sprintf("%d", int(r.Code)))})
				if e != nil {
					panic(e)
				}
				sm.store(p)
			default:
				panic(r)
			}
		}
	}()

	var e error

	op := string(inst[0].([]byte))
	if sm.verbose {
		fmt.Printf("Instruction is %s (%v)\n", op, sm.prefix)
		fmt.Printf("Stack from [")
		sm.dumpStack()
		fmt.Printf(" ]\n")
	}

	var obj interface{}
	switch {
	case strings.HasSuffix(op, "_SNAPSHOT"):
		obj = sm.tr.Snapshot()
		op = op[:len(op)-9]
	case strings.HasSuffix(op, "_DATABASE"):
		obj = db
		op = op[:len(op)-9]
	default:
		obj = sm.tr
	}

	switch string(op) {
	case "PUSH":
		sm.store(inst[1])
	case "DUP":
		sm.store(sm.stack[len(sm.stack) - 1])
	case "EMPTY_STACK":
		sm.stack = []interface{}{}
		sm.stack = make([]interface{}, 0)
	case "SWAP":
		idx := sm.waitAndPop().(int64)
		sm.stack[len(sm.stack) - 1], sm.stack[len(sm.stack) - 1 - int(idx)] = sm.stack[len(sm.stack) - 1 - int(idx)], sm.stack[len(sm.stack) - 1]
	case "POP":
		sm.stack = sm.stack[:len(sm.stack) - 1]
	case "SUB":
		sm.store(sm.waitAndPop().(int64) - sm.waitAndPop().(int64))
	case "NEW_TRANSACTION":
		sm.tr, e = db.CreateTransaction()
		if e != nil {
			panic(e)
		}
		for k, _ := range sm.atomics {
			delete(sm.atomics, k)
		}
		sm.atomics["ADD"] = sm.tr.Add
		sm.atomics["AND"] = sm.tr.And
		sm.atomics["OR"] = sm.tr.Or
		sm.atomics["XOR"] = sm.tr.Xor
	case "ON_ERROR":
		sm.store(sm.tr.OnError(fdb.NewError(int(sm.waitAndPop().(int64)))))
	case "GET_READ_VERSION":
		switch o := obj.(type) {
		case fdb.ReadTransaction:
			sm.lastVersion = o.GetReadVersion().GetOrPanic()
			sm.store([]byte("GOT_READ_VERSION"))
		}
	case "SET":
		switch o := obj.(type) {
		case *fdb.Database:
			e = o.Set(sm.waitAndPop().([]byte), sm.waitAndPop().([]byte))
			if e != nil {
				panic(e)
			}
			sm.store([]byte("RESULT_NOT_PRESENT"))
		case *fdb.Transaction:
			o.Set(sm.waitAndPop().([]byte), sm.waitAndPop().([]byte))
		}
	case "GET":
		switch o := obj.(type) {
		case *fdb.Database:
			v, e := db.Get(sm.waitAndPop().([]byte))
			if e != nil {
				panic(e)
			}
			if v != nil {
				sm.store(v)
			} else {
				sm.store([]byte("RESULT_NOT_PRESENT"))
			}
		case fdb.ReadTransaction:
			sm.store(o.Get(sm.waitAndPop().([]byte)))
		}
	case "COMMIT":
		sm.store(sm.tr.Commit())
	case "RESET":
		sm.tr.Reset()
	case "CLEAR":
		switch o := obj.(type) {
		case *fdb.Database:
			e := db.Clear(sm.waitAndPop().([]byte))
			if e != nil {
				panic(e)
			}
		case *fdb.Transaction:
			o.Clear(sm.waitAndPop().([]byte))
		}
	case "SET_READ_VERSION":
		sm.tr.SetReadVersion(sm.lastVersion)
	case "WAIT_FUTURE":
		sm.store(sm.waitAndPop())
	case "GET_COMMITTED_VERSION":
		sm.lastVersion, e = sm.tr.GetCommittedVersion()
		if e != nil {
			panic(e)
		}
		sm.store([]byte("GOT_COMMITTED_VERSION"))
	case "GET_KEY":
		sel := fdb.KeySelector{sm.waitAndPop().([]byte), int64ToBool(sm.waitAndPop().(int64)), int(sm.waitAndPop().(int64))}
		switch o := obj.(type) {
		case *fdb.Database:
			v, e := o.GetKey(sel)
			if e != nil {
				panic(e)
			}
			sm.store(v)
		case fdb.ReadTransaction:
			sm.store(o.GetKey(sel))
		}
	case "GET_RANGE":
		begin := sm.waitAndPop().([]byte)
		end := sm.waitAndPop().([]byte)
		var limit int
		switch l := sm.waitAndPop().(type) {
		case int64:
			limit = int(l)
		}
		reverse := int64ToBool(sm.waitAndPop().(int64))
		mode := sm.waitAndPop().(int64)
		switch o := obj.(type) {
		case *fdb.Database:
			v, e := db.GetRange(begin, end, fdb.RangeOptions{Limit: int(limit), Reverse: reverse, Mode: fdb.StreamingMode(mode+1)})
			if e != nil {
				panic(e)
			}
			sm.pushRange(v)
		case fdb.ReadTransaction:
			sm.pushRange(o.GetRange(begin, end, fdb.RangeOptions{Limit: int(limit), Reverse: reverse, Mode: fdb.StreamingMode(mode+1)}).GetSliceOrPanic())
		}
	case "CLEAR_RANGE":
		switch o := obj.(type) {
		case *fdb.Database:
			e := o.ClearRange(sm.waitAndPop().([]byte), sm.waitAndPop().([]byte))
			if e != nil {
				panic(e)
			}
			sm.store([]byte("RESULT_NOT_PRESENT"))
		case *fdb.Transaction:
			o.ClearRange(sm.waitAndPop().([]byte), sm.waitAndPop().([]byte))
		}
	case "GET_RANGE_STARTS_WITH":
		prefix := sm.waitAndPop().([]byte)
		var limit int
		switch l := sm.waitAndPop().(type) {
		case int64:
			limit = int(l)
		}
		reverse := int64ToBool(sm.waitAndPop().(int64))
		mode := sm.waitAndPop().(int64)
		switch o := obj.(type) {
		case *fdb.Database:
			v, e := db.GetRangeStartsWith(prefix, fdb.RangeOptions{Limit: int(limit), Reverse: reverse, Mode: fdb.StreamingMode(mode)})
			if e != nil {
				panic(e)
			}
			sm.pushRange(v)
		case fdb.ReadTransaction:
			sm.pushRange(o.GetRangeStartsWith(prefix, fdb.RangeOptions{Limit: int(limit), Reverse: reverse, Mode: fdb.StreamingMode(mode)}).GetSliceOrPanic())
		}
	case "GET_RANGE_SELECTOR":
		begin := fdb.KeySelector{Key: sm.waitAndPop().([]byte), OrEqual: int64ToBool(sm.waitAndPop().(int64)), Offset: int(sm.waitAndPop().(int64))}
		end := fdb.KeySelector{Key: sm.waitAndPop().([]byte), OrEqual: int64ToBool(sm.waitAndPop().(int64)), Offset: int(sm.waitAndPop().(int64))}
		var limit int
		switch l := sm.waitAndPop().(type) {
		case int64:
			limit = int(l)
		}
		reverse := int64ToBool(sm.waitAndPop().(int64))
		mode := sm.waitAndPop().(int64)
		switch o := obj.(type) {
		case *fdb.Database:
			v, e := db.GetRangeSelector(begin, end, fdb.RangeOptions{Limit: int(limit), Reverse: reverse, Mode: fdb.StreamingMode(mode)})
			if e != nil {
				panic(e)
			}
			sm.pushRange(v)
		case fdb.ReadTransaction:
			sm.pushRange(o.GetRangeSelector(begin, end, fdb.RangeOptions{Limit: int(limit), Reverse: reverse, Mode: fdb.StreamingMode(mode)}).GetSliceOrPanic())
		}
	case "CLEAR_RANGE_STARTS_WITH":
		prefix := sm.waitAndPop().([]byte)
		switch o := obj.(type) {
		case *fdb.Database:
			e := o.ClearRangeStartsWith(prefix)
			if e != nil {
				panic(e)
			}
			sm.store([]byte("RESULT_NOT_PRESENT"))
		case *fdb.Transaction:
			o.ClearRangeStartsWith(prefix)
		}
	case "TUPLE_PACK":
		var t tuple.Tuple
		count := sm.waitAndPop().(int64)
		for i := 0; i < int(count); i++ {
			t = append(t, sm.waitAndPop())
		}
		p, e := tuple.Pack(t)
		if e != nil {
			panic(e)
		}
		sm.store(p)
	case "TUPLE_UNPACK":
		t, e := tuple.Unpack(sm.waitAndPop().([]byte))
		if e != nil {
			panic(e)
		}
		for _, el := range(t) {
			p, e := tuple.Pack(tuple.Tuple{el})
			if e != nil {
				panic(e)
			}
			sm.store(p)
		}
	case "TUPLE_RANGE":
		var t tuple.Tuple
		count := sm.waitAndPop().(int64)
		for i := 0; i < int(count); i++ {
			t = append(t, sm.waitAndPop())
		}
		begin, end, e := tuple.Range(t)
		if e != nil {
			panic(e)
		}
		sm.store(begin)
		sm.store(end)
	case "START_THREAD":
		newsm := newStackMachine(sm.waitAndPop().([]byte), verbose)
		sm.threads.Add(1)
		go func() {
			newsm.Run()
			sm.threads.Done()
		}()
	case "WAIT_EMPTY":
		prefix := sm.waitAndPop().([]byte)
		db.Transact(func (tr *fdb.Transaction) (interface{}, error) {
			v := tr.GetRangeStartsWith(prefix, fdb.RangeOptions{}).GetSliceOrPanic()
			if len(v) != 0 {
				panic(fdb.NewError(1020))
			}
			return nil, nil
		})
		sm.store([]byte("WAITED_FOR_EMPTY"))
	case "READ_CONFLICT_RANGE":
		e = sm.tr.AddReadConflictRange(sm.waitAndPop().([]byte), sm.waitAndPop().([]byte))
		if e != nil {
			panic(e)
		}
		sm.store([]byte("SET_CONFLICT_RANGE"))
	case "WRITE_CONFLICT_RANGE":
		e = sm.tr.AddWriteConflictRange(sm.waitAndPop().([]byte), sm.waitAndPop().([]byte))
		if e != nil {
			panic(e)
		}
		sm.store([]byte("SET_CONFLICT_RANGE"))
	case "READ_CONFLICT_KEY":
		e = sm.tr.AddReadConflictKey(sm.waitAndPop().([]byte))
		if e != nil {
			panic(e)
		}
		sm.store([]byte("SET_CONFLICT_KEY"))
	case "WRITE_CONFLICT_KEY":
		e = sm.tr.AddWriteConflictKey(sm.waitAndPop().([]byte))
		if e != nil {
			panic(e)
		}
		sm.store([]byte("SET_CONFLICT_KEY"))
	case "ATOMIC_OP":
		opname := string(sm.waitAndPop().([]byte))
		switch obj.(type) {
		case *fdb.Database:
			dbAtomics[opname](sm.waitAndPop().([]byte), sm.waitAndPop().([]byte))
		case *fdb.Transaction:
			sm.atomics[opname](sm.waitAndPop().([]byte), sm.waitAndPop().([]byte))
		}
	case "DISABLE_WRITE_CONFLICT":
		sm.tr.Options.SetNextWriteNoWriteConflictRange()
	case "CANCEL":
		sm.tr.Cancel()
	case "UNIT_TESTS":
	default:
		log.Fatalf("Unhandled operation %s\n", string(inst[0].([]byte)))
	}

	if sm.verbose {
		fmt.Printf("        to [")
		sm.dumpStack()
		fmt.Printf(" ]\n\n")
	}

	runtime.Gosched()
}

func (sm *StackMachine) Run() {
	begin, end, e := tuple.Range(tuple.Tuple{sm.prefix})
	if e != nil {
		panic(e)
	}

	r, e := db.Transact(func (tr *fdb.Transaction) (interface{}, error) {
		return tr.GetRange(begin, end, fdb.RangeOptions{}).GetSliceOrPanic(), nil
	})
	if e != nil {
		panic(e)
	}

	instructions := r.([]fdb.KeyValue)

	for i, kv := range(instructions) {
		inst, _ := tuple.Unpack(kv.Value)

		if sm.verbose {
			fmt.Printf("Instruction %d\n", i)
		}
		sm.processInst(inst)
	}

	sm.threads.Wait()
}

var config fdb.DBConfig

var db *fdb.Database
var dbAtomics map[string]func([]byte, []byte) error

func main() {
	prefix := []byte(os.Args[1])
	if len(os.Args) > 2 {
		config.ClusterFile = os.Args[2]
	}

	var e error

	api, e := fdb.APIVersion(100)
	if e != nil {
		log.Fatal(e)
	}

	config.DBName = []byte("DB")
	db, e = api.Open(&config)
	if e != nil {
		log.Fatal(e)
	}

	dbAtomics = make(map[string]func([]byte, []byte) error)
	dbAtomics["ADD"] = db.Add
	dbAtomics["AND"] = db.And
	dbAtomics["OR"] = db.Or
	dbAtomics["XOR"] = db.Xor

	sm := newStackMachine(prefix, verbose)

	sm.Run()
}
