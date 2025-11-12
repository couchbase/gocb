package gocb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10"

	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) TestScopeQuery() {
	suite.skipIfUnsupported(CollectionsQueryFeature)

	n := suite.setupScopeQuery()
	suite.Run("TestScopeQuery", func() {
		suite.runScopeQueryPositionalTest(n, true)
		suite.runScopeQueryNamedTest(n, true)
	})
	suite.Run("TestScopeQueryNoMetrics", func() {
		suite.runScopeQueryPositionalTest(n, false)
		suite.runScopeQueryNamedTest(n, false)
	})
	suite.Run("TestScopePreparedQuery", func() {
		suite.runScopePreparedQueryPositionalTest(n)
		suite.runScopePreparedQueryNamedTest(n)
	})
}

func (suite *IntegrationTestSuite) setupScopeQuery() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "scopequery", globalScope.Name(),
		globalCollection.Name())
	suite.Require().Nil(err, "Failed to create dataset %v", err)

	mgr := globalCollection.QueryIndexes()
	err = mgr.CreatePrimaryIndex(&CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
		Timeout:        30 * time.Second,
	})
	suite.Require().Nil(err, "Failed to create index %v", err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "#primary", globalCollection.bucketName(),
		globalCollection.ScopeName(), globalCollection.Name(), func(row queryRow) bool {
			return row.State == "online"
		})

	return n
}

func (suite *IntegrationTestSuite) runScopePreparedQueryPositionalTest(n int) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalCollection.Name(), globalCollection.Name(), n)
	suite.runPreparedQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, []interface{}{"scopequery"}, nil)
}

func (suite *IntegrationTestSuite) runScopePreparedQueryNamedTest(n int) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service LIMIT %d;", globalCollection.Name(), globalCollection.Name(), n)
	suite.runPreparedQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, nil, map[string]interface{}{"service": "scopequery"})
}

func (suite *IntegrationTestSuite) runScopePreparedQueryBothPositionalAndNamedTest(n int, withMetrics bool) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service AND type=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runPreparedQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, []interface{}{"brewery"}, map[string]interface{}{"service": "query"})
}

func (suite *IntegrationTestSuite) runScopeQueryPositionalTest(n int, withMetrics bool) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalCollection.Name(), globalCollection.Name(), n)
	suite.runQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, withMetrics, []interface{}{"scopequery"}, nil)
}

func (suite *IntegrationTestSuite) runScopeQueryNamedTest(n int, withMetrics bool) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service LIMIT %d;", globalCollection.Name(), globalCollection.Name(), n)
	suite.runQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, withMetrics, nil, map[string]interface{}{"service": "scopequery"})
}

func (suite *IntegrationTestSuite) runScopeQueryBothPositionalAndNamedTest(n int, withMetrics bool) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service AND type=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, withMetrics, []interface{}{"brewery"}, map[string]interface{}{"service": "query"})
}

func (suite *UnitTestSuite) queryScope(prepared bool, retryStrategy *coreRetryStrategyWrapper, reader queryRowReader, runFn func(args mock.Arguments)) *Scope {
	provider, call := suite.newMockQueryProvider(prepared, reader)
	call.Run(runFn)

	queryProvider := &queryProviderCore{
		provider: provider,
		tracer:   newTracerWrapper(&NoopTracer{}),
	}

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("getMeter").Return(nil)
	cli.On("MarkOpBeginning").Return()
	cli.On("MarkOpCompleted").Return()

	b := suite.bucket("queryBucket", cli)

	scope := suite.newScope(b, "queryScope")

	queryProvider.retryStrategyWrapper = retryStrategy
	queryProvider.timeouts.QueryTimeout = 75000 * time.Millisecond

	return scope
}

func (suite *UnitTestSuite) TestScopeQueryPrepared() {
	var dataset testQueryDataset
	err := loadJSONTestDataset("beer_sample_query_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockQueryRowReader{
		Dataset: dataset.Results,
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			Meta:  suite.mustConvertToBytes(dataset.jsonQueryResponse),
			Suite: suite,
			PName: dataset.jsonQueryResponse.Prepared,
		},
	}

	statement := "SELECT * FROM dataset"

	rs := newCoreRetryStrategyWrapper(NewBestEffortRetryStrategy(nil))
	var scope *Scope
	scope = suite.queryScope(true, rs, reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.N1QLQueryOptions)
		suite.Assert().Equal(rs, opts.RetryStrategy)
		now := time.Now()
		if opts.Deadline.Before(now.Add(70*time.Second)) || opts.Deadline.After(now.Add(75*time.Second)) {
			suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
		}

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Contains(actualOptions, "client_context_id")
		suite.Assert().Equal(actualOptions["query_context"], "default:`queryBucket`.`queryScope`")
	})

	result, err := scope.Query(statement, nil)
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	suite.assertQueryBeerResult(dataset, result)
}

func (suite *IntegrationTestSuite) TestScopeQueryTransaction() {
	suite.skipIfUnsupported(QueryFeature)
	suite.skipIfUnsupported(TransactionsFeature)

	mgr := globalCollection.QueryIndexes()
	err := mgr.CreatePrimaryIndex(&CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	// Ensure the index is online
	suite.Eventually(func() bool {
		res, err := globalScope.Query(fmt.Sprintf("SELECT 1 FROM %s", globalCollection.Name()), &QueryOptions{
			Adhoc: true,
		})
		if err != nil {
			return false
		}

		for res.Next() {
		}

		err = res.Err()
		return err == nil
	}, 30*time.Second, 500*time.Millisecond)

	docID := uuid.New().String()
	res, err := globalScope.Query(fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {})", globalCollection.Name(), docID), &QueryOptions{
		AsTransaction: &SingleQueryTransactionOptions{
			DurabilityLevel: DurabilityLevelMajority,
		},
		Adhoc: true,
	})
	suite.Require().Nil(err, err)

	for res.Next() {
	}

	err = res.Err()
	suite.Require().Nil(err, err)

	meta, err := res.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Equal(uint64(1), meta.Metrics.MutationCount)

	// Verify that we've inserted into the correct place.
	getRes, err := globalCollection.Get(docID, &GetOptions{
		Transcoder: NewRawJSONTranscoder(),
	})
	suite.Require().Nil(err, err)

	var getResBytes []byte
	err = getRes.Content(&getResBytes)
	suite.Require().Nil(err, err)

	suite.Assert().Equal([]byte("{}"), getResBytes)
}

func (suite *IntegrationTestSuite) TestScopeQueryTransactionDoubleInsert() {
	suite.skipIfUnsupported(QueryFeature)
	suite.skipIfUnsupported(TransactionsFeature)

	mgr := globalCollection.QueryIndexes()
	err := mgr.CreatePrimaryIndex(&CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	suite.Eventually(func() bool {
		res, err := globalScope.Query(fmt.Sprintf("SELECT 1 FROM %s", globalCollection.Name()), &QueryOptions{
			Adhoc: true,
		})
		if err != nil {
			return false
		}

		for res.Next() {
		}

		err = res.Err()
		suite.Require().Nil(err, err)
		return err == nil
	}, 30*time.Second, 500*time.Millisecond)

	docID := uuid.New().String()
	res, err := globalScope.Query(fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {})", globalCollection.Name(), docID), &QueryOptions{
		AsTransaction: &SingleQueryTransactionOptions{
			DurabilityLevel: DurabilityLevelMajority,
		},
		Adhoc: true,
	})
	suite.Require().Nil(err, err)

	for res.Next() {
	}

	err = res.Err()
	suite.Require().Nil(err, err)

	meta, err := res.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Equal(uint64(1), meta.Metrics.MutationCount)

	_, err = globalScope.Query(fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {})", globalCollection.Name(), docID), &QueryOptions{
		AsTransaction: &SingleQueryTransactionOptions{
			DurabilityLevel: DurabilityLevelMajority,
		},
		Adhoc: true,
	})

	if globalCluster.SupportsFeature(TransactionsSingleQueryExistsErrorFeature) {
		var tErr *TransactionFailedError
		if errors.As(err, &tErr) {
			suite.T().Logf("Error should have not have been TransactionFailed but was: %v", err)
			suite.T().Fail()
		}

		suite.Require().ErrorIs(err, ErrDocumentExists)
	} else {
		var tErr *TransactionFailedError
		suite.Assert().ErrorAs(err, &tErr)
	}
}
