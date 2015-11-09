package gocbcore

import (
	"sync/atomic"
	"unsafe"
)

type routeData struct {
	revId uint

	queues     []*memdQueue
	vbMap      [][]int
	capiEpList []string
	mgmtEpList []string
	n1qlEpList []string

	servers        []*memdPipeline
	pendingServers []*memdPipeline
	waitQueue      *memdQueue
	deadQueue      *memdQueue

	source *routeConfig
}

func (d *routeData) logDebug() {
	logDebugf("  Revision ID: %d", d.revId)

	logDebugf("  Queues:")
	for i, q := range d.queues {
		if q == nil {
			logDebugf("    %d: nil", i)
		}

		if q == d.waitQueue {
			logDebugf("    %d: WaitQueue", i)
			continue
		}

		var ownerServer int = -1
		for j, s := range d.servers {
			if q == s.queue {
				ownerServer = j
				break
			}
		}
		if ownerServer >= 0 {
			logDebugf("    %d: Server %d", i, ownerServer)
			continue
		}

		logDebugf("    %d: Unknown... %v", i, q)
	}

	logDebugf("  Servers:")
	for i, s := range d.servers {
		if s == nil {
			logDebugf("    %d: nil", i)
		} else if !s.IsClosed() {
			logDebugf("    %d: %p[%s] (ACTIVE)", i, s, s.Address())
		} else {
			logDebugf("    %d: %p[%s] (CLOSED)", i, s, s.Address())
		}
	}

	logDebugf("  Pending Servers:")
	for i, s := range d.pendingServers {
		if s == nil {
			logDebugf("    %d: nil", i)
		} else {
			logDebugf("    %d: %p[%s]", i, s, s.Address())
		}
	}

	if d.waitQueue != nil {
		logDebugf("  Has WaitQueue? YES")
	} else {
		logDebugf("  Has WaitQueue? NO")
	}

	if d.deadQueue != nil {
		logDebugf("  Has DeadQueue? YES")
	} else {
		logDebugf("  Has DeadQueue? NO")
	}

	logDebugf("  Capi Eps:")
	for _, ep := range d.capiEpList {
		logDebugf("    - %s", ep)
	}
	logDebugf("  Mgmt Eps:")
	for _, ep := range d.mgmtEpList {
		logDebugf("    - %s", ep)
	}
	logDebugf("  N1ql Eps:")
	for _, ep := range d.n1qlEpList {
		logDebugf("    - %s", ep)
	}

	//logDebugf("  Source Data: %v", d.source)
}

type routeDataPtr struct {
	data unsafe.Pointer
}

func (ptr *routeDataPtr) get() *routeData {
	return (*routeData)(atomic.LoadPointer(&ptr.data))
}

func (ptr *routeDataPtr) update(old, new *routeData) bool {
	if new == nil {
		panic("Attempted to update to nil routeData")
	}
	if old != nil {
		return atomic.CompareAndSwapPointer(&ptr.data, unsafe.Pointer(old), unsafe.Pointer(new))
	} else {
		if atomic.SwapPointer(&ptr.data, unsafe.Pointer(new)) != nil {
			panic("Updated from nil attempted on initialized routeDataPtr")
		}
		return true
	}
}

func (ptr *routeDataPtr) clear() *routeData {
	val := atomic.SwapPointer(&ptr.data, nil)
	if val == nil {
		panic("Attempted to clear a nil routeDataPtr")
	}
	return (*routeData)(val)
}
