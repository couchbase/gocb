package main

import (
	"time"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/counter"
)

type boundsExecutor interface {
	CanExecute() bool
}

type counterBoundsExecutor struct {
	counter *counter.Counter
}

func newCounterBoundsExecutor(counter *counter.Counter) *counterBoundsExecutor {
	return &counterBoundsExecutor{
		counter: counter,
	}
}

func (executor *counterBoundsExecutor) CanExecute() bool {
	return executor.counter.GetAndDecrement() >= 0
}

type timeBoundsExecutor struct {
	deadline time.Time
}

func newTimeBoundsExecutor(deadline time.Time) *timeBoundsExecutor {
	return &timeBoundsExecutor{
		deadline: deadline,
	}
}

func (executor *timeBoundsExecutor) CanExecute() bool {
	return time.Now().Before(executor.deadline)
}
