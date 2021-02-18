package gocb

import (
	"context"
	gocbcore "github.com/couchbase/gocbcore/v9"
)

type asyncOpManager struct {
	signal chan struct{}

	wasResolved bool
}

func (m *asyncOpManager) Reject() {
	m.signal <- struct{}{}
}

func (m *asyncOpManager) Resolve() {
	m.wasResolved = true
	m.signal <- struct{}{}
}

func (m *asyncOpManager) Wait(ctx context.Context, op gocbcore.PendingOp) {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-m.signal:
		// Good to go
	case <-ctx.Done():
		op.Cancel()
		<-m.signal
	}
}

func newAsyncOpManager() *asyncOpManager {
	return &asyncOpManager{
		signal: make(chan struct{}, 1),
	}
}
