package fdb

import (
	"C"
)

//export notifyChannel
func notifyChannel(ch *chan struct{}) {
	*ch <- struct{}{}
}
