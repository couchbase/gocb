package gocb

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Ensures that operations beginning concurrently with close fast-fail at MarkOpBeginning with ErrShutdown, if the
// connection manager's close method is blocked waiting for active operations to finish.
// Validates fix for GOCBC-1855.
func (suite *UnitTestSuite) TestConnectionMgrMarkOpBeginningDuringClose() {
	type opTracker interface {
		opController
		markClosed() error
		waitForActiveOps()
	}

	connectionManagers := map[string]opTracker{
		"stdConnectionMgr": &stdConnectionMgr{},
		"psConnectionMgr":  &psConnectionMgr{},
	}

	for name, connMgr := range connectionManagers {
		suite.Run(name, func() {
			var wg sync.WaitGroup
			var started sync.WaitGroup
			stop := make(chan struct{})

			started.Add(8)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					first := true
					for {
						select {
						case <-stop:
							return
						default:
						}

						if err := connMgr.MarkOpBeginning(); err != nil {
							suite.Assert().ErrorIs(err, ErrShutdown)
							return
						}
						connMgr.MarkOpCompleted()

						if first {
							first = false
							started.Done()
						}
					}
				}()
			}

			// Wait until every goroutine has completed at least one op, so that close
			// can genuinely race against in-flight MarkOpBeginning calls.
			started.Wait()

			suite.Require().NoError(connMgr.markClosed())
			suite.Assert().ErrorIs(connMgr.MarkOpBeginning(), ErrShutdown)

			connMgr.waitForActiveOps()

			suite.Require().ErrorIs(connMgr.MarkOpBeginning(), ErrShutdown)

			close(stop)
			wg.Wait()
		})
	}
}

type closedOpController struct{}

func (c closedOpController) MarkOpBeginning() error { return ErrShutdown }
func (c closedOpController) MarkOpCompleted()       {}

func (suite *UnitTestSuite) TestAutoOpControlWithClosedOpController() {
	mockKvController := &providerController[kvProvider]{
		get: func() (kvProvider, error) {
			return nil, errors.New("should not have got here")
		},
		opController: closedOpController{},
	}

	res, err := autoOpControl(mockKvController, "DummyOperation", func(kvProvider) (int, error) {
		return 10, nil
	})
	suite.Assert().ErrorIs(err, ErrShutdown)
	suite.Assert().Equal(0, res)
}

func (suite *UnitTestSuite) TestAutoOpControlMarksOpCompletedWhenGetProviderFails() {
	guard := &activeOpGuard{}
	mockKvController := &providerController[kvProvider]{
		get: func() (kvProvider, error) {
			return nil, errors.New("could not get provider")
		},
		opController: guard,
	}

	res, err := autoOpControl(mockKvController, "DummyOperation", func(kvProvider) (int, error) {
		return 10, errors.New("should not have got here")
	})
	suite.Require().EqualError(err, "could not get provider")
	suite.Assert().Equal(0, res)

	suite.Require().NoError(guard.markClosed())

	waitReturned := make(chan struct{})
	go func() {
		guard.waitForActiveOps()
		close(waitReturned)
	}()

	select {
	case <-waitReturned:
	case <-time.After(5 * time.Second):
		suite.Fail("waitForActiveOps did not return, the failed operation did not call MarkOpCompleted")
	}
}

func (suite *UnitTestSuite) TestActiveOpGuardMarkClosedOnlySucceedsOnce() {
	guard := activeOpGuard{}

	suite.Require().NoError(guard.canPerformOp())
	suite.Require().NoError(guard.markClosed())

	suite.Assert().ErrorIs(guard.markClosed(), ErrShutdown)
	suite.Assert().ErrorIs(guard.canPerformOp(), ErrShutdown)
	suite.Assert().ErrorIs(guard.MarkOpBeginning(), ErrShutdown)
}

func (suite *UnitTestSuite) TestActiveOpGuardWaitForActiveOpsWaitsForInFlightOp() {
	guard := activeOpGuard{}

	suite.Require().NoError(guard.MarkOpBeginning())

	waitReturned := make(chan struct{})
	go func() {
		guard.waitForActiveOps()
		close(waitReturned)
	}()

	select {
	case <-waitReturned:
		suite.Fail("waitForActiveOps returned whilst an operation was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	guard.MarkOpCompleted()

	select {
	case <-waitReturned:
	case <-time.After(5 * time.Second):
		suite.Fail("waitForActiveOps did not return after the operation completed")
	}
}

func (suite *UnitTestSuite) TestActiveOpGuardWaitForActiveOpsReturnsWhenIdle() {
	guard := activeOpGuard{}

	suite.Require().NoError(guard.MarkOpBeginning())
	guard.MarkOpCompleted()

	waitReturned := make(chan struct{})
	go func() {
		guard.waitForActiveOps()
		close(waitReturned)
	}()

	select {
	case <-waitReturned:
	case <-time.After(5 * time.Second):
		suite.Fail("waitForActiveOps did not return with no operations in flight")
	}
}

func (suite *UnitTestSuite) TestActiveOpGuardRejectsOpsAfterWaitForActiveOpsReturns() {
	const (
		iterations = 1000
		workers    = 8

		// Only ever needed when the guard is broken in a way that waitForActiveOps doesn't return
		timeout = 30 * time.Second
	)

	for _ = range iterations {
		guard := &activeOpGuard{}

		var waitForActiveOpsReturned atomic.Bool
		var opAdmittedAfterWait atomic.Bool
		var wg sync.WaitGroup

		// Operations stop when the guard starts refusing them. The stop channel exists just in case a guard which
		// never refuses anything fails this test rather than hanging.
		stop := make(chan struct{})

		for _ = range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}

					if err := guard.MarkOpBeginning(); err != nil {
						suite.Assert().ErrorIs(err, ErrShutdown)
						return
					}

					if waitForActiveOpsReturned.Load() {
						opAdmittedAfterWait.Store(true)
						guard.MarkOpCompleted()
						return
					}
					guard.MarkOpCompleted()
				}
			}()
		}

		waitReturned := make(chan struct{})
		go func() {
			defer close(waitReturned)

			suite.Assert().NoError(guard.markClosed())
			guard.waitForActiveOps()
			waitForActiveOpsReturned.Store(true)
		}()

		select {
		case <-waitReturned:
		case <-time.After(timeout):
			close(stop)
			suite.FailNow("waitForActiveOps did not return, the read lock for at least one op was " +
				"never released")
		}

		close(stop)

		workersReturned := make(chan struct{})
		go func() {
			defer close(workersReturned)

			wg.Wait()
		}()

		select {
		case <-workersReturned:
		case <-time.After(timeout):
			suite.FailNow("operations did not stop after the guard was closed")
		}

		if opAdmittedAfterWait.Load() {
			suite.FailNow("An operation was admitted after markClosed & waitForActiveOps")
		}
	}
}
