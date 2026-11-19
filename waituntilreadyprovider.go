package gocb

import (
	"context"
	"time"
)

type waitUntilReadyProvider interface {
	WaitUntilReady(ctx context.Context, deadline time.Time, opts *WaitUntilReadyOptions) error
}
