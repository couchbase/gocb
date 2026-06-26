package main

import (
	"errors"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/counter"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/meta"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"

	protoTxns "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/transactions"
)

type Executor interface {
	PerformOperation(command *sdk.Command, sender sender.ResultSender) (bool, error)
}

type TransactionsExecutor interface {
	PerformOperation(command *protoTxns.TransactionCreateRequest) (*run.Result, error)
}

type PerHorizontalRunner struct {
	RunnerIndex int
	Sender      sender.ResultSender
	Workloads   []*run.Workload
}

type HorizontalScaleRunner struct {
	logger   *logrus.Logger
	executor Executor
	counters *counter.Counters
	per      PerHorizontalRunner

	transactionExecutor TransactionsExecutor

	doneCh chan struct{}

	successCount uint32
	failCount    uint32
	err          error
}

func NewHorizontalScaleRunner(logger *logrus.Logger, executor Executor, counters *counter.Counters, per PerHorizontalRunner) *HorizontalScaleRunner {
	return &HorizontalScaleRunner{
		executor: executor,
		logger:   logger,
		counters: counters,
		per:      per,

		doneCh: make(chan struct{}),
	}
}

func (runner *HorizontalScaleRunner) SetTransactionExecutor(executor TransactionsExecutor) {
	runner.transactionExecutor = executor
}

func (runner *HorizontalScaleRunner) Wait() {
	<-runner.doneCh
}

func (runner *HorizontalScaleRunner) Err() error {
	return runner.err
}

func (runner *HorizontalScaleRunner) Run() {
	runner.logger.Logf(logrus.InfoLevel, "Runner thread has started, will run %d workloads", len(runner.per.Workloads))
	defer runner.close()

	for _, workload := range runner.per.Workloads {
		switch w := workload.Workload.(type) {
		case *run.Workload_Sdk:
			err := runner.runSDKWorkload(w)
			if err != nil {
				runner.logger.Logf(logrus.ErrorLevel, "Runner thread died with: %v", err)
				return
			}
		case *run.Workload_Transaction:
			err := runner.runTransactionWorkload(w)
			if err != nil {
				runner.logger.Logf(logrus.ErrorLevel, "Runner thread died with: %v", err)
				return
			}
		case *run.Workload_Grpc:
			err := runner.runGRPCWorkload(w)
			if err != nil {
				runner.logger.Logf(logrus.ErrorLevel, "Runner thread died with: %v", err)
				return
			}
		default:
			runner.logger.Log(logrus.WarnLevel, "Runner thread encountered unknown workload type")
		}
	}

	runner.logger.Logf(logrus.InfoLevel, "Runner thread finished after %d successful operations and %d failed",
		runner.successCount, runner.failCount)
}

func (runner *HorizontalScaleRunner) runSDKWorkload(workload *run.Workload_Sdk) error {
	numCommands := len(workload.Sdk.Command)
	c, err := runner.bounds(workload.Sdk.Bounds, numCommands)
	if err != nil {
		return err
	}

	var executed int
	for c.CanExecute() {
		nextCommand := workload.Sdk.Command[executed%numCommands]
		executed += 1

		success, err := runner.executor.PerformOperation(nextCommand, runner.per.Sender)
		if success {
			runner.successCount += 1
		} else {
			runner.failCount += 1
		}
		if err != nil {
			runner.err = err
			break
		}
	}

	return nil
}

func (runner *HorizontalScaleRunner) runTransactionWorkload(workload *run.Workload_Transaction) error {
	numCommands := len(workload.Transaction.Command)
	c, err := runner.bounds(workload.Transaction.Bounds, numCommands)
	if err != nil {
		return err
	}

	var executed int
	for c.CanExecute() {
		nextCommand := workload.Transaction.Command[executed%numCommands]
		executed += 1

		res, err := runner.transactionExecutor.PerformOperation(nextCommand)
		if err != nil {
			return err
		}
		runner.per.Sender.Send(res)
		if res.GetTransaction() != nil {
			if res.GetTransaction().GetException() == protoTxns.TransactionException_NO_EXCEPTION_THROWN {
				runner.successCount += 1
			} else {
				runner.failCount += 1
			}
		}
	}

	return nil
}

func (runner *HorizontalScaleRunner) runGRPCWorkload(workload *run.Workload_Grpc) error {
	c, err := runner.bounds(workload.Grpc.Bounds, 1)
	if err != nil {
		return err
	}

	for c.CanExecute() {
		switch workload.Grpc.Command.Command.(type) {
		case *meta.Command_Ping:
		default:
			return errors.New("unknown grpc command type")
		}

		runner.per.Sender.Send(&run.Result{
			Result: &run.Result_Grpc{
				Grpc: &meta.Result{
					Result: &meta.Result_PingResult{
						PingResult: &meta.PingResult{},
					},
				},
			},
		})
	}

	return nil
}

func (runner *HorizontalScaleRunner) bounds(bounds *shared.Bounds, defaultCounter int) (boundsExecutor, error) {
	if bounds == nil {
		return newCounterBoundsExecutor(counter.NewCounter(int32(defaultCounter))), nil
	}

	switch b := bounds.Bounds.(type) {
	case *shared.Bounds_Counter:
		c, err := runner.counters.Get(b.Counter)
		if err != nil {
			return nil, err
		}

		runner.logger.Infof("Runner thread will run commands until counter `%s` is 0, currently %d",
			b.Counter.GetCounterId(),
			c.Get())

		return newCounterBoundsExecutor(c), nil

	case *shared.Bounds_ForTime:
		return newTimeBoundsExecutor(time.Now().Add(time.Duration(b.ForTime.Seconds) * time.Second)), nil

	case *shared.Bounds_CounterEq:
		c, err := runner.counters.Get(b.CounterEq)
		if err != nil {
			return nil, err
		}

		runner.logger.Infof("Runner thread will run commands while counter `%s` is unchanged, currently %d",
			b.CounterEq.GetCounterId(), c.Get())

		return newCounterEqualityBoundsExecutor(c), nil

	default:
		return nil, errors.New("unknown bounds type")
	}
}

func (runner *HorizontalScaleRunner) close() {
	close(runner.doneCh)
}
