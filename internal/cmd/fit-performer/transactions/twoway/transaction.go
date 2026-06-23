package twoway

import (
	"encoding/json"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/cluster"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/transactions/countdownlatch"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/query"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	protoTransactions "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/transactions"
)

type Transaction struct {
	latches    map[string]*countdownlatch.CountdownLatch
	conn       *cluster.Connection
	stashed    *gocb.TransactionGetResult
	logger     *logrus.Logger
	fatalError error
}

func New(conn *cluster.Connection, logger *logrus.Logger) *Transaction {
	return &Transaction{
		conn:    conn,
		latches: make(map[string]*countdownlatch.CountdownLatch),
		logger:  logger,
	}
}

func (txn *Transaction) AddLatches(pLatches []*shared.Latch) {
	for _, latch := range pLatches {
		txn.logger.Logf(logrus.InfoLevel, "Adding latch %s with count of %d", latch.Name, latch.InitialCount)
		txn.latches[latch.Name] = countdownlatch.New(latch.InitialCount)
	}
}

func (txn *Transaction) CountdownLatch(name string) error {
	latch, ok := txn.latches[name]
	if !ok {
		return errors.New("no latch with given id exists")
	}
	latch.CountDown()
	return nil
}

func (txn *Transaction) Run(factory *gocb.Transactions, expectedEvents []*protoTransactions.Event,
	attempts []*protoTransactions.TransactionAttemptRequest, config *gocb.TransactionOptions,
	stream protocol.PerformerService_TransactionStreamServer) (*protoTransactions.TransactionResult, error) {
	// Would need to setup the event listener here
	attemptCount := -1
	numAttempts := float64(len(attempts) - 1)
	if numAttempts < 0 {
		numAttempts = 0
	}
	result, err := factory.Run(func(txnCtx *gocb.TransactionAttemptContext) error {
		attemptCount++
		attemptToUse := int(math.Min(float64(attemptCount), numAttempts))
		attempt := attempts[attemptToUse]
		for _, command := range attempt.Commands {
			// This error may have to be fed upstream differently to this, maybe via outer scope
			err := txn.executeCommand(txnCtx, command, txn.conn, stream)
			if err != nil {
				return err
			}
		}
		return nil
	}, config)
	if txn.fatalError != nil {
		txn.logger.Logf(logrus.ErrorLevel, "Test really failed: %v", txn.fatalError)
		return nil, txn.fatalError
	}
	var exception protoTransactions.TransactionException
	if err != nil {
		txn.logger.Logf(logrus.InfoLevel, "Run returned error: %v", err)
		switch txnErr := err.(type) {
		case *gocb.TransactionFailedError:
			result = txnErr.Result()
			exception = protoTransactions.TransactionException_EXCEPTION_FAILED
		case *gocb.TransactionExpiredError:
			result = txnErr.Result()
			exception = protoTransactions.TransactionException_EXCEPTION_EXPIRED
		case *gocb.TransactionCommitAmbiguousError:
			result = txnErr.Result()
			exception = protoTransactions.TransactionException_EXCEPTION_COMMIT_AMBIGUOUS
		default:
			txn.logger.Log(logrus.ErrorLevel, "Test returned unexpected error")
			return nil, err
		}
	}
	logs := make([]string, len(result.Logs))
	for i, log := range result.Logs {
		logs[i] = log.String()
	}

	res := &protoTransactions.TransactionResult{
		TransactionId:          result.TransactionID,
		UnstagingComplete:      result.UnstagingComplete,
		Exception:              exception,
		ExceptionCause:         mapFinalErrorCause(err),
		CleanupRequestsPending: factory.Internal().CleanupQueueLength(),
		CleanupRequestsValid:   factory.Internal().ClientCleanupEnabled(),
		Log:                    logs,
	}
	txn.logger.Logf(logrus.InfoLevel, "Final run result: %+v", res)
	return res, nil
}

func (txn *Transaction) getStashedDoc() *gocb.TransactionGetResult {
	return txn.stashed
}

func (txn *Transaction) setStashedDoc(doc *gocb.TransactionGetResult) {
	txn.stashed = doc
}

func (txn *Transaction) executeCommand(txnCtx *gocb.TransactionAttemptContext,
	command *protoTransactions.TransactionCommand, conn *cluster.Connection,
	stream protocol.PerformerService_TransactionStreamServer) error {
	switch c := command.GetCommand().(type) {
	case *protoTransactions.TransactionCommand_Insert:
		col := conn.Collection(c.Insert.DocId.BucketName, c.Insert.DocId.ScopeName, c.Insert.DocId.CollectionName)
		txn.logger.Logf(logrus.InfoLevel, "Performing insert on doc %s on %s.%s.%s", c.Insert.DocId.DocId, c.Insert.DocId.BucketName, col.ScopeName(), col.Name())
		var val interface{}

		switch opt := c.Insert.ContentOption.(type) {
		case *protoTransactions.CommandInsert_ContentJson:
			err := json.Unmarshal([]byte(opt.ContentJson), &val)
			if err != nil {
				return err
			}
		case *protoTransactions.CommandInsert_Content:
			var err error
			val, err = helpers.ContentFromShared(opt.Content)
			if err != nil {
				return err
			}
		default:
			return errors.New("unsupported content option")
		}

		opts, err := toGocbTxnInsertOptions(c.Insert)
		if err != nil {
			return err
		}
		_, err = txnCtx.InsertWithOptions(col, c.Insert.DocId.DocId, val, opts)

		return txn.verifyExpectations("Insert "+c.Insert.DocId.DocId, c.Insert.ExpectedResult, err, command.DoNotPropagateError)
	case *protoTransactions.TransactionCommand_Replace:
		var doc *gocb.TransactionGetResult
		if c.Replace.UseStashedResult {
			txn.logger.Logf(logrus.InfoLevel, "Using stashed get for replace")
			doc = txn.getStashedDoc()
			if doc == nil {
				txn.logger.Log(logrus.ErrorLevel, "Get for replace failed, no document fetched yet")
				return errors.New("no document has been fetched yet")
			}
		} else {
			bName := c.Replace.DocId.BucketName
			col := conn.Collection(bName, c.Replace.DocId.ScopeName, c.Replace.DocId.CollectionName)
			txn.logger.Logf(logrus.InfoLevel, "Performing get for replace on doc %s on %s.%s", c.Replace.DocId.DocId, bName, col.Name())
			var err error
			doc, err = txnCtx.Get(col, c.Replace.DocId.DocId)
			if err != nil {
				txn.logger.Logf(logrus.ErrorLevel, "Get for replace failed: %v", err)
				return txn.verifyExpectations("Get for Replace", c.Replace.ExpectedResult, err, command.DoNotPropagateError)
			}
		}
		var val interface{}

		switch opt := c.Replace.ContentOption.(type) {
		case *protoTransactions.CommandReplace_ContentJson:
			err := json.Unmarshal([]byte(opt.ContentJson), &val)
			if err != nil {
				return err
			}
		case *protoTransactions.CommandReplace_Content:
			var err error
			val, err = helpers.ContentFromShared(opt.Content)
			if err != nil {
				return err
			}
		default:
			return errors.New("unsupported content option")
		}
		txn.logger.Log(logrus.InfoLevel, "Performing replace")

		opts, err := toGocbTxnReplaceOptions(c.Replace)
		if err != nil {
			return err
		}
		_, err = txnCtx.ReplaceWithOptions(doc, val, opts)

		return txn.verifyExpectations("Replace", c.Replace.ExpectedResult, err, command.DoNotPropagateError)
	case *protoTransactions.TransactionCommand_Remove:
		var doc *gocb.TransactionGetResult
		if c.Remove.UseStashedResult {
			txn.logger.Log(logrus.InfoLevel, "Using stashed get for remove")
			doc = txn.getStashedDoc()
			if doc == nil {
				return errors.New("no document has been fetched yet")
			}
		} else {
			bName := c.Remove.DocId.BucketName
			col := conn.Collection(bName, c.Remove.DocId.ScopeName, c.Remove.DocId.CollectionName)
			txn.logger.Logf(logrus.InfoLevel, "Performing get for remove on doc %s on %s.%s", c.Remove.DocId.DocId, bName, col.Name())
			var err error
			doc, err = txnCtx.Get(col, c.Remove.DocId.DocId)
			if err != nil {
				txn.logger.Logf(logrus.ErrorLevel, "Get for remove failed: %v", err)
				return err
			}
		}
		txn.logger.Log(logrus.InfoLevel, "Performing remove")
		err := txnCtx.Remove(doc)
		return txn.verifyExpectations("Remove", c.Remove.ExpectedResult, err, command.DoNotPropagateError)
	case *protoTransactions.TransactionCommand_Commit:
		txn.logger.Log(logrus.InfoLevel, "Performing commit")
		// err := txnCtx.Commit()
		//
		// return txn.verifyExpectations("Commit", c.Commit.ExpectedResult, err, command.DoNotPropagateError)
		return nil
	case *protoTransactions.TransactionCommand_Rollback:
		txn.logger.Log(logrus.InfoLevel, "Performing rollback")
		// err := txnCtx.Rollback()
		//
		// return txn.verifyExpectations("Rollback", c.Rollback.ExpectedResult, err, command.DoNotPropagateError)
		return errors.New("rollmeback")
	case *protoTransactions.TransactionCommand_Get:
		bName := c.Get.DocId.BucketName
		col := conn.Collection(bName, c.Get.DocId.ScopeName, c.Get.DocId.CollectionName)
		txn.logger.Logf(logrus.InfoLevel, "Performing get on doc %s on %s.%s.%s", c.Get.DocId.DocId, bName, c.Get.DocId.ScopeName, c.Get.DocId.CollectionName)

		var opts *gocb.TransactionGetOptions
		if c.Get.Options != nil && c.Get.Options.Transcoder != nil {
			opts = &gocb.TransactionGetOptions{}
			t, err := helpers.Transcoder(c.Get.Options.Transcoder)
			if err != nil {
				return err
			}

			opts.Transcoder = t
		}
		result, err := txnCtx.GetWithOptions(col, c.Get.DocId.DocId, opts)

		// do error verification first
		err = txn.verifyExpectations("Get "+c.Get.DocId.DocId, c.Get.ExpectedResult, err, command.DoNotPropagateError)
		if err != nil {
			return err
		}
		txn.setStashedDoc(result)
		err = txn.verifyGetResultExpectedContentJSON(result, c.Get.ExpectedContentJson)
		if err != nil {
			return err
		}
		return txn.verifyGetResult(result, c.Get.ContentAsValidation)

	case *protoTransactions.TransactionCommand_GetFromPreferredServerGroup:
		bName := c.GetFromPreferredServerGroup.DocId.BucketName
		col := conn.Collection(bName, c.GetFromPreferredServerGroup.DocId.ScopeName, c.GetFromPreferredServerGroup.DocId.CollectionName)
		txn.logger.Logf(logrus.InfoLevel, "Performing get on doc %s on %s.%s.%s", c.GetFromPreferredServerGroup.DocId.DocId, bName, c.GetFromPreferredServerGroup.DocId.ScopeName, c.GetFromPreferredServerGroup.DocId.CollectionName)
		result, err := txnCtx.GetReplicaFromPreferredServerGroup(col, c.GetFromPreferredServerGroup.DocId.DocId)
		// do error verification first
		err = txn.verifyExpectations("GetFromPreferredServerGroup "+c.GetFromPreferredServerGroup.DocId.DocId, c.GetFromPreferredServerGroup.ExpectedResult, err, command.DoNotPropagateError)
		if err != nil {
			return err
		}
		txn.setStashedDoc(result)
		return txn.verifyGetResult(result, c.GetFromPreferredServerGroup.ContentAsValidation)

	case *protoTransactions.TransactionCommand_GetOptional:
		bName := c.GetOptional.Get.DocId.BucketName
		col := conn.Collection(bName, c.GetOptional.Get.DocId.ScopeName, c.GetOptional.Get.DocId.CollectionName)
		txn.logger.Logf(logrus.InfoLevel, "Performing getoptional on doc %s on %s.%s", c.GetOptional.Get.DocId.DocId, bName, col.Name())

		var opts *gocb.TransactionGetOptions
		if c.GetOptional.Options != nil && c.GetOptional.Options.Transcoder != nil {
			opts = &gocb.TransactionGetOptions{}
			t, err := helpers.Transcoder(c.GetOptional.Options.Transcoder)
			if err != nil {
				return err
			}

			opts.Transcoder = t
		}
		result, err := txnCtx.GetWithOptions(col, c.GetOptional.Get.DocId.DocId, opts)

		if errors.Is(err, gocb.ErrDocumentNotFound) {
			err = nil
		}
		// do error verification first
		err = txn.verifyExpectations("GetOptional "+c.GetOptional.Get.DocId.DocId, c.GetOptional.Get.ExpectedResult, err, command.DoNotPropagateError)
		if err != nil {
			return err
		}
		txn.setStashedDoc(result)
		err = txn.verifyGetResultExpectedContentJSON(result, c.GetOptional.Get.ExpectedContentJson)
		if err != nil {
			return err
		}
		return txn.verifyGetResult(result, c.GetOptional.ContentAsValidation)

	case *protoTransactions.TransactionCommand_GetMulti:
		if c.GetMulti.GetMultiReplicasFromPreferredServerGroup {
			specs, err := toGocbTxnBulkGetReplicaSpecs(conn, c.GetMulti)
			if err != nil {
				return err
			}
			txn.logger.Logf(logrus.InfoLevel, "Performing BulkGetReplicaFromPreferredServerGroup for %d docs", len(specs))
			result, err := txnCtx.BulkGetReplicaFromPreferredServerGroup(specs, toGocbTxnBulkGetReplicaOptions(c.GetMulti))
			err = txn.verifyExpectations(fmt.Sprintf("BulkGetReplicaFromPreferredServerGroup %d docs", len(specs)), c.GetMulti.ExpectedResult, err, command.DoNotPropagateError)
			if err != nil {
				return err
			}
			return txn.verifyBulkGetResult(result, c.GetMulti.GetSpecs())
		} else {
			specs, err := toGocbTxnBulkGetSpecs(conn, c.GetMulti)
			if err != nil {
				return err
			}
			txn.logger.Logf(logrus.InfoLevel, "Performing BulkGet for %d docs", len(specs))
			result, err := txnCtx.BulkGet(specs, toGocbTxnBulkGetOptions(c.GetMulti))
			err = txn.verifyExpectations(fmt.Sprintf("BulkGet %d docs", len(specs)), c.GetMulti.ExpectedResult, err, command.DoNotPropagateError)
			if err != nil {
				return err
			}
			return txn.verifyBulkGetResult(result, c.GetMulti.GetSpecs())
		}

	case *protoTransactions.TransactionCommand_WaitOnLatch:
		if stream == nil {
			return errors.New("WaitOnLatch used without stream")
		}
		latch, ok := txn.latches[c.WaitOnLatch.LatchName]
		if !ok {
			return errors.New("no latch with given id exists")
		}
		txn.logger.Logf(logrus.InfoLevel, "Blocking on latch %s, current count = %d", c.WaitOnLatch.LatchName, latch.Count())
		latch.Await()
		txn.logger.Logf(logrus.InfoLevel, "Blocking finished on latch %s", c.WaitOnLatch.LatchName)
		return nil
	case *protoTransactions.TransactionCommand_SetLatch:
		if stream == nil {
			return errors.New("WaitOnLatch used without stream")
		}
		latch, ok := txn.latches[c.SetLatch.LatchName]
		if !ok {
			return errors.New("no latch with given id exists")
		}
		txn.logger.Logf(logrus.InfoLevel, "Counting down on latch %s, current count = %d", c.SetLatch.LatchName, latch.Count())
		latch.CountDown()
		txn.logger.Logf(logrus.InfoLevel, "Broadcasting setlatch for %s", c.SetLatch.LatchName)
		return stream.Send(&protoTransactions.TransactionStreamPerformerToDriver{
			Response: &protoTransactions.TransactionStreamPerformerToDriver_Broadcast{
				Broadcast: &protoTransactions.BroadcastToOtherConcurrentTransactionsRequest{
					Request: &protoTransactions.BroadcastToOtherConcurrentTransactionsRequest_LatchSet{
						LatchSet: &protoTransactions.CommandSetLatch{
							LatchName: c.SetLatch.LatchName,
						},
					},
				},
			},
		})
	case *protoTransactions.TransactionCommand_Parallelize:
		var failedErr atomic.Pointer[error]
		txn.logger.Logf(logrus.InfoLevel, "Running %d commands in parallel with concurrency = %d", len(c.Parallelize.Commands), c.Parallelize.Parallelism)
		var wg sync.WaitGroup
		wg.Add(len(c.Parallelize.Commands))
		queue := make(chan *protoTransactions.TransactionCommand, len(c.Parallelize.Commands))
		for _, command := range c.Parallelize.Commands {
			queue <- command
		}
		close(queue)
		for i := 0; i < int(c.Parallelize.Parallelism); i++ {
			go func() {
				for {
					command, more := <-queue
					if !more {
						return
					}
					err := txn.executeCommand(txnCtx, command, conn, stream)
					if err != nil {
						failedErr.CompareAndSwap(nil, &err)
					}
					wg.Done()
				}
			}()
		}
		wg.Wait()
		if failedErr.Load() != nil {
			txn.logger.Log(logrus.WarnLevel, "Not all ops succeeded, rolling back with error: ", *failedErr.Load())
			return *failedErr.Load()
		}

		return nil
	case *protoTransactions.TransactionCommand_InsertRegularKv:
		col := conn.Collection(c.InsertRegularKv.DocId.BucketName, c.InsertRegularKv.DocId.ScopeName, c.InsertRegularKv.DocId.CollectionName)
		txn.logger.Logf(logrus.InfoLevel, "Performing regular kv insert on doc %s on %s.%s", c.InsertRegularKv.DocId.DocId,
			c.InsertRegularKv.DocId.BucketName, col.Name())
		var val interface{}
		if err := json.Unmarshal([]byte(c.InsertRegularKv.ContentJson), &val); err != nil {
			return err
		}
		_, err := col.Insert(c.InsertRegularKv.DocId.DocId, val, nil)
		if err != nil {
			return err
		}
		return nil
	case *protoTransactions.TransactionCommand_ReplaceRegularKv:
		col := conn.Collection(c.ReplaceRegularKv.DocId.BucketName, c.ReplaceRegularKv.DocId.ScopeName, c.ReplaceRegularKv.DocId.CollectionName)
		txn.logger.Logf(logrus.InfoLevel, "Performing regular kv replace on doc %s on %s.%s", c.ReplaceRegularKv.DocId.DocId,
			c.ReplaceRegularKv.DocId.BucketName, col.Name())
		var val interface{}
		if err := json.Unmarshal([]byte(c.ReplaceRegularKv.ContentJson), &val); err != nil {
			return err
		}
		_, err := col.Replace(c.ReplaceRegularKv.DocId.DocId, val, nil)
		if err != nil {
			return err
		}
		return nil
	case *protoTransactions.TransactionCommand_RemoveRegularKv:
		col := conn.Collection(c.RemoveRegularKv.DocId.BucketName, c.RemoveRegularKv.DocId.ScopeName, c.RemoveRegularKv.DocId.CollectionName)
		txn.logger.Logf(logrus.InfoLevel, "Performing regular kv remove on doc %s on %s.%s", c.RemoveRegularKv.DocId.DocId,
			c.RemoveRegularKv.DocId.BucketName, col.Name())
		_, err := col.Remove(c.RemoveRegularKv.DocId.DocId, nil)
		if err != nil {
			return err
		}
		return nil
	case *protoTransactions.TransactionCommand_Query:
		txn.logger.Logf(logrus.InfoLevel, "Performing query %s", c.Query.Statement)
		var scope *gocb.Scope
		if c.Query.Scope != nil {
			scope = conn.Bucket(c.Query.Scope.BucketName).Scope(c.Query.Scope.ScopeName)
		}
		res, err := txnCtx.Query(c.Query.Statement, gocbTxnQueryOptionsFromProto(c.Query.QueryOptions, scope))
		if err := txn.verifyQueryResult(res, err, c.Query, command.DoNotPropagateError); err != nil {
			return err
		}
		return nil
	case *protoTransactions.TransactionCommand_ThrowException:
		txn.logger.Logf(logrus.InfoLevel, "Throwing exception")
		return errors.New("returning error as directed by ThrowException command")
	case *protoTransactions.TransactionCommand_TestFail:
		txn.logger.Logf(logrus.InfoLevel, "Failing test")
		err := errors.New("failing test as directed by TestFail command")
		txn.fatalError = err
		return err
	default:
		return errors.New("unsupported transaction command")
	}
}

func (txn *Transaction) SingleQuery(conn *gocb.Cluster, cfg *query.QueryOptions, asTransaction gocb.SingleQueryTransactionOptions,
	c *protoTransactions.CommandQuery) (*protoTransactions.TransactionSingleQueryResponse, error) {

	opts := gocbSingleQueryOptionsFromProto(cfg, asTransaction)

	var res *gocb.QueryResult
	var err error
	if c.Scope == nil {
		res, err = conn.Query(c.GetStatement(), opts)
	} else {
		res, err = conn.Bucket(c.Scope.GetBucketName()).Scope(c.Scope.GetScopeName()).Query(c.GetStatement(), opts)
	}

	verified, err := txn.verifySingleQueryResult(res, err, c)
	if err != nil {
		return nil, err
	}

	return verified, nil
}

func gocbSingleQueryOptionsFromProto(protoOpts *query.QueryOptions, asTransaction gocb.SingleQueryTransactionOptions) *gocb.QueryOptions {
	if protoOpts == nil {
		return &gocb.QueryOptions{
			AsTransaction: &asTransaction,
		}
	}

	var posParams []interface{}
	if len(protoOpts.ParametersPositional) > 0 {
		posParams = make([]interface{}, len(protoOpts.ParametersPositional))
		for i, param := range protoOpts.ParametersPositional {
			posParams[i] = param
		}
	}

	var namedParams map[string]interface{}
	if len(protoOpts.ParametersNamed) > 0 {
		namedParams = make(map[string]interface{}, len(protoOpts.ParametersNamed))
		for key, param := range protoOpts.ParametersNamed {
			namedParams[key] = param
		}
	}

	var raw map[string]interface{}
	if len(protoOpts.Raw) > 0 {
		raw = make(map[string]interface{}, len(protoOpts.Raw))
		for key, param := range protoOpts.Raw {
			raw[key] = param
		}
	}

	return &gocb.QueryOptions{
		ScanConsistency:      helpers.ScanConsistencyToGocb(protoOpts.GetScanConsistency()),
		Profile:              gocb.QueryProfileMode(protoOpts.GetProfile()),
		ScanCap:              uint32(protoOpts.GetScanCap()),
		PipelineBatch:        uint32(protoOpts.GetPipelineBatch()),
		PipelineCap:          uint32(protoOpts.GetPipelineCap()),
		ScanWait:             time.Duration(protoOpts.GetScanWaitMillis()) * time.Millisecond,
		Readonly:             protoOpts.GetReadonly(),
		MaxParallelism:       uint32(protoOpts.GetMaxParallelism()),
		PositionalParameters: posParams,
		NamedParameters:      namedParams,
		Metrics:              protoOpts.GetMetrics(),
		Raw:                  raw,
		Adhoc:                protoOpts.Adhoc == nil || protoOpts.GetAdhoc(),
		Timeout:              time.Duration(protoOpts.GetTimeoutMillis()) * time.Millisecond,
		FlexIndex:            protoOpts.GetFlexIndex(),
		AsTransaction:        &asTransaction,
	}
}

func gocbTxnQueryOptionsFromProto(protoOpts *protoTransactions.TransactionQueryOptions, scope *gocb.Scope) *gocb.TransactionQueryOptions {
	if protoOpts == nil && scope == nil {
		return nil
	}

	if protoOpts == nil {
		return &gocb.TransactionQueryOptions{
			Scope: scope,
		}
	}

	var posParams []interface{}
	if len(protoOpts.ParametersPositional) > 0 {
		posParams = make([]interface{}, len(protoOpts.ParametersPositional))
		for i, param := range protoOpts.ParametersPositional {
			posParams[i] = param
		}
	}

	var namedParams map[string]interface{}
	if len(protoOpts.ParametersNamed) > 0 {
		namedParams = make(map[string]interface{}, len(protoOpts.ParametersNamed))
		for key, param := range protoOpts.ParametersNamed {
			namedParams[key] = param
		}
	}

	var raw map[string]interface{}
	if len(protoOpts.Raw) > 0 {
		raw = make(map[string]interface{}, len(protoOpts.Raw))
		for key, param := range protoOpts.Raw {
			raw[key] = param
		}
	}

	return &gocb.TransactionQueryOptions{
		ScanConsistency:      helpers.ScanConsistencyToGocb(protoOpts.GetScanConsistency()),
		Profile:              gocb.QueryProfileMode(protoOpts.GetProfile()),
		ScanCap:              uint32(protoOpts.GetScanCap()),
		PipelineBatch:        uint32(protoOpts.GetPipelineBatch()),
		PipelineCap:          uint32(protoOpts.GetPipelineCap()),
		ScanWait:             time.Duration(protoOpts.GetScanWaitMillis()) * time.Millisecond,
		Readonly:             protoOpts.GetReadonly(),
		PositionalParameters: posParams,
		NamedParameters:      namedParams,
		Raw:                  raw,
		Prepared:             !protoOpts.GetAdhoc(),
		FlexIndex:            protoOpts.GetFlexIndex(),
		Scope:                scope,
		ClientContextID:      protoOpts.GetClientContextId(),
	}
}
