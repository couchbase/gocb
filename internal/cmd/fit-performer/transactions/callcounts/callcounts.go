package callcounts

import (
	"sync"

	protoHooksTxns "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/hooks/transactions"
)

type CallCounts struct {
	hookCount          map[protoHooksTxns.HookPoint]int32
	hookCountWithParam map[protoHooksTxns.HookPoint]map[string]int32
	lock               sync.Mutex
}

func NewCallCounts() *CallCounts {
	return &CallCounts{
		hookCount:          make(map[protoHooksTxns.HookPoint]int32),
		hookCountWithParam: make(map[protoHooksTxns.HookPoint]map[string]int32),
	}
}

func (cc *CallCounts) Increment(point protoHooksTxns.HookPoint) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if _, ok := cc.hookCount[point]; !ok {
		cc.hookCount[point] = 0
	}

	cc.hookCount[point]++
}

func (cc *CallCounts) IncrementWithParam(point protoHooksTxns.HookPoint, param string) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if _, ok := cc.hookCountWithParam[point]; !ok {
		cc.hookCountWithParam[point] = make(map[string]int32)
	}
	if _, ok := cc.hookCountWithParam[point][param]; !ok {
		cc.hookCountWithParam[point][param] = 0
	}

	cc.hookCountWithParam[point][param]++
}

func (cc *CallCounts) Count(point protoHooksTxns.HookPoint) int32 {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if count, ok := cc.hookCount[point]; ok {
		return count
	}

	return 0
}

func (cc *CallCounts) CountWithParam(point protoHooksTxns.HookPoint, param string) int32 {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if p, ok := cc.hookCountWithParam[point]; ok {
		if count, ok := p[param]; ok {
			return count
		}
	}

	return 0
}
