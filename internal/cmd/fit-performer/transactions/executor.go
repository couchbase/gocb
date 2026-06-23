package transactions

import (
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/sirupsen/logrus"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/cluster"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/counter"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/transactions/twoway"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/transactions"
)

type Executor struct {
	conn     *cluster.Connection
	counters *counter.Counters
	logger   *logrus.Logger
}

func NewExecutor(conn *cluster.Connection, counters *counter.Counters, logger *logrus.Logger) *Executor {
	return &Executor{
		conn:     conn,
		counters: counters,
		logger:   logger,
	}
}

func (e *Executor) PerformOperation(command *transactions.TransactionCreateRequest) (*run.Result, error) {
	conn := e.conn

	var opts *gocb.TransactionOptions
	if command.Options != nil {
		var metaCollection *gocb.Collection
		c := command.Options.MetadataCollection
		if c != nil {
			metaCollection = conn.Bucket(c.BucketName).Scope(c.ScopeName).Collection(c.CollectionName)
		}

		opts = &gocb.TransactionOptions{
			DurabilityLevel:    helpers.ProtocolDuraToSDK(command.Options.GetDurability()),
			Timeout:            time.Duration(command.Options.GetTimeoutMillis()) * time.Millisecond,
			MetadataCollection: metaCollection,
		}
	}

	txn := twoway.New(conn, e.logger)
	res, err := txn.Run(conn.Transactions(), command.ExpectedEvents, command.Attempts, opts, nil)
	if err != nil {
		// Something really went wrong here
		return nil, err
	}

	return &run.Result{
		Result: &run.Result_Transaction{
			Transaction: res,
		},
	}, nil
}
