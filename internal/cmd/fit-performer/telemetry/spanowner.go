package telemetry

import (
	"github.com/couchbase/gocb/v2"
	"sync"
)

type SpanOwner struct {
	spans map[string]gocb.RequestSpan
	lock  sync.Mutex
}

func NewSpanOwner() *SpanOwner {
	return &SpanOwner{
		spans: make(map[string]gocb.RequestSpan),
	}
}

func (so *SpanOwner) GetSpan(id string) (gocb.RequestSpan, bool) {
	so.lock.Lock()
	span, ok := so.spans[id]
	so.lock.Unlock()

	return span, ok
}

func (so *SpanOwner) StoreSpan(id string, span gocb.RequestSpan) {
	so.lock.Lock()
	so.spans[id] = span
	so.lock.Unlock()
}
