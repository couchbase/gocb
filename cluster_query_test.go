package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/mock"
)

type testQueryDataset struct {
	Results []testBreweryDocument
	jsonQueryResponse
}

type queryIface interface {
	Query(string, *QueryOptions) (*QueryResult, error)
}

func (suite *IntegrationTestSuite) TestQuery() {
	suite.skipIfUnsupported(QueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	n := suite.setupClusterQuery()
	suite.Run("TestClusterQuery", func() {
		suite.runClusterQueryPositionalTest(n, true)
		suite.runClusterQueryNamedTest(n, true)
		suite.runClusterQueryBothPositionalAndNamedTest(n, true)
	})
	suite.Run("TestClusterQueryNoMetrics", func() {
		suite.runClusterQueryPositionalTest(n, false)
		suite.runClusterQueryNamedTest(n, false)
		suite.runClusterQueryBothPositionalAndNamedTest(n, false)
	})
	suite.Run("TestClusterPreparedQuery", func() {
		suite.runClusterPreparedQueryPositionalTest(n)
		suite.runClusterPreparedQueryNamedTest(n)
		suite.runClusterPreparedQueryBothPositionalAndNamedTest(n)
	})
}

func (suite *IntegrationTestSuite) runPreparedQueryTest(n int, query, bucket, scope string, queryFn queryIface, positionalParams []interface{}, namedParams map[string]interface{}) {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	deadline := time.Now().Add(60 * time.Second)
	for {
		globalTracer.Reset()
		globalMeter.Reset()
		contextID := "contextID"
		opts := &QueryOptions{
			Timeout:              5 * time.Second,
			ClientContextID:      contextID,
			PositionalParameters: positionalParams,
			NamedParameters:      namedParams,
		}
		result, err := queryFn.Query(query, opts)
		suite.Require().Nil(err, "Failed to execute query %v", err)

		suite.Require().Contains(globalTracer.GetSpans(), nil)
		nilParents := globalTracer.GetSpans()[nil]
		suite.Require().Equal(1, len(nilParents))

		var numDispatchSpans int
		if globalCluster.IsProtostellar() {
			numDispatchSpans = 1
		} else {
			numDispatchSpans = 1
			if !globalCluster.SupportsFeature(EnhancedPreparedStatementsFeature) {
				// Old style prepared statements means 2 requests.
				numDispatchSpans = 2
			}
		}
		suite.AssertHTTPOpSpan(nilParents[0], "query",
			HTTPOpSpanExpectations{
				bucket:                  bucket,
				scope:                   scope,
				statement:               query,
				numDispatchSpans:        numDispatchSpans,
				atLeastNumDispatchSpans: false,
				hasEncoding:             !globalCluster.IsProtostellar(),
				service:                 "query",
				dispatchOperationID:     contextID,
			})

		suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "query"), 1, false)

		var samples []interface{}
		for result.Next() {
			var sample interface{}
			err := result.Row(&sample)
			suite.Require().Nil(err, "Failed to get value from row %v", err)
			samples = append(samples, sample)
		}

		err = result.Err()
		suite.Require().Nil(err, "Result had error %v", err)

		metadata, err := result.MetaData()
		suite.Require().Nil(err, "Metadata had error: %v", err)

		suite.Assert().NotEmpty(metadata.RequestID)

		if n == len(samples) {
			return
		}

		sleepDeadline := time.Now().Add(1000 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))

		if sleepDeadline == deadline {
			suite.T().Errorf("timed out waiting for indexing")
			return
		}
	}
}

func (suite *IntegrationTestSuite) runClusterPreparedQueryPositionalTest(n int) {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runPreparedQueryTest(n, query, "", "", globalCluster, []interface{}{"query"}, nil)
}

func (suite *IntegrationTestSuite) runClusterPreparedQueryNamedTest(n int) {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runPreparedQueryTest(n, query, "", "", globalCluster, nil, map[string]interface{}{"service": "query"})
}

func (suite *IntegrationTestSuite) runClusterPreparedQueryBothPositionalAndNamedTest(n int) {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service AND type=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runPreparedQueryTest(n, query, "", "", globalCluster, []interface{}{"brewery"}, map[string]interface{}{"service": "query"})
}

func (suite *IntegrationTestSuite) TestClusterQueryImprovedErrorsDocNotFound() {
	suite.skipIfUnsupported(QueryImprovedErrorsFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	suite.setupClusterQuery()
	query := fmt.Sprintf("INSERT INTO `%s` (KEY, VALUE) VALUES (\"%s\", \"test\")", globalBucket.Name(), uuid.New().String())

	success := suite.tryUntil(time.Now().Add(60*time.Second), 500*time.Millisecond, func() bool {
		res, err := globalCluster.Query(query, nil)
		if err != nil && !errors.Is(err, ErrIndexNotFound) {
			suite.T().Logf("Error performing query: %v", err)
			return false
		}

		for res.Next() {
		}

		err = res.Err()
		if err != nil && !errors.Is(err, ErrIndexNotFound) {
			suite.T().Logf("Error performing query: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Timed out waiting for query to succeed")

	res, err := globalCluster.Query(query, nil)
	suite.Require().Nil(err, err)

	for res.Next() {
	}

	err = res.Err()
	suite.Require().ErrorIs(err, ErrDocumentExists)

	var qErr *QueryError
	suite.Require().ErrorAs(err, &qErr)
	suite.Require().Len(qErr.Errors, 1)
	suite.Assert().Equal(uint32(12009), qErr.Errors[0].Code)
	suite.Assert().Equal(float64(17012), qErr.Errors[0].Reason["code"])
}

func (suite *IntegrationTestSuite) runQueryTest(n int, query, bucket, scope string, queryFn queryIface, withMetrics bool, positionalParams []interface{}, namedParams map[string]interface{}) {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	deadline := time.Now().Add(60 * time.Second)
	for {
		globalTracer.Reset()
		globalMeter.Reset()
		contextID := "contextID"
		opts := &QueryOptions{
			Timeout:              5 * time.Second,
			Adhoc:                true,
			Metrics:              withMetrics,
			ClientContextID:      contextID,
			PositionalParameters: positionalParams,
			NamedParameters:      namedParams,
		}
		result, err := queryFn.Query(query, opts)
		suite.Require().Nil(err, "Failed to execute query %v", err)

		suite.Require().Contains(globalTracer.GetSpans(), nil)
		nilParents := globalTracer.GetSpans()[nil]
		suite.Require().Equal(1, len(nilParents))

		suite.AssertHTTPOpSpan(nilParents[0], "query",
			HTTPOpSpanExpectations{
				bucket:                  bucket,
				scope:                   scope,
				statement:               query,
				numDispatchSpans:        1,
				atLeastNumDispatchSpans: false,
				hasEncoding:             !globalCluster.IsProtostellar(),
				service:                 "query",
				dispatchOperationID:     contextID,
			})

		suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "query"), 1, false)

		var samples []interface{}
		for result.Next() {
			var sample interface{}
			err := result.Row(&sample)
			suite.Require().Nil(err, "Failed to get value from row %v", err)
			samples = append(samples, sample)
		}

		err = result.Err()
		suite.Require().Nil(err, "Result had error %v", err)

		metadata, err := result.MetaData()
		suite.Require().Nil(err, "Metadata had error: %v", err)

		suite.Assert().NotEmpty(metadata.RequestID)

		if withMetrics {
			suite.Assert().NotZero(metadata.Metrics.ElapsedTime)
			suite.Assert().NotZero(metadata.Metrics.ExecutionTime)
		}

		if n == len(samples) {
			if withMetrics {
				suite.Assert().NotZero(metadata.Metrics.ResultCount)
				suite.Assert().NotZero(metadata.Metrics.ResultSize)
			}
			return
		}

		sleepDeadline := time.Now().Add(1000 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))

		if sleepDeadline == deadline {
			suite.T().Errorf("timed out waiting for indexing")
			return
		}
	}
}

func (suite *IntegrationTestSuite) runClusterQueryPositionalTest(n int, withMetrics bool) {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runQueryTest(n, query, "", "", globalCluster, withMetrics, []interface{}{"query"}, nil)
}

func (suite *IntegrationTestSuite) runClusterQueryNamedTest(n int, withMetrics bool) {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runQueryTest(n, query, "", "", globalCluster, withMetrics, nil, map[string]interface{}{"service": "query"})
}

func (suite *IntegrationTestSuite) runClusterQueryBothPositionalAndNamedTest(n int, withMetrics bool) {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service AND type=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runQueryTest(n, query, "", "", globalCluster, withMetrics, []interface{}{"brewery"}, map[string]interface{}{"service": "query"})
}

func (suite *IntegrationTestSuite) setupClusterQuery() int {
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "query", "", "")
	suite.Require().Nil(err, "Failed to create dataset %v", err)

	mgr := globalCluster.QueryIndexes()
	err = mgr.CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
		Timeout:        30 * time.Second,
	})
	suite.Require().Nil(err, "Failed to create index %v", err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "#primary", globalBucket.Name(),
		"", "", func(row queryRow) bool {
			return row.State == "online"
		})

	return n
}

func (suite *IntegrationTestSuite) TestClusterQueryContext() {
	suite.skipIfUnsupported(QueryFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := globalCluster.Query("SELECT 1=1", &QueryOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
	defer cancel()

	res, err = globalCluster.Query("SELECT 1=1", &QueryOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) && !errors.Is(err, ErrTimeout) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)
}

func (suite *IntegrationTestSuite) TestClusterQueryTransaction() {
	suite.skipIfUnsupported(QueryFeature)
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	mgr := globalCluster.QueryIndexes()
	err := mgr.CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	suite.Eventually(func() bool {
		res, err := globalCluster.Query(fmt.Sprintf("SELECT 1 FROM %s", globalBucket.Name()), &QueryOptions{
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

	res, err := globalCluster.Query(fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {})", globalBucket.Name(), uuid.New().String()), &QueryOptions{
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
}

func (suite *IntegrationTestSuite) TestClusterQueryTransactionDoubleInsert() {
	suite.skipIfUnsupported(QueryFeature)
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	mgr := globalCluster.QueryIndexes()
	err := mgr.CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	suite.Eventually(func() bool {
		res, err := globalCluster.Query(fmt.Sprintf("SELECT 1 FROM %s", globalBucket.Name()), &QueryOptions{
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
	res, err := globalCluster.Query(fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {})", globalBucket.Name(), docID), &QueryOptions{
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

	_, err = globalCluster.Query(fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {})", globalBucket.Name(), docID), &QueryOptions{
		AsTransaction: &SingleQueryTransactionOptions{
			DurabilityLevel: DurabilityLevelMajority,
		},
		Adhoc: true,
	})
	suite.Require().Error(err, err)

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

func (suite *IntegrationTestSuite) TestClusterQueryTransactionOne() {
	suite.skipIfUnsupported(QueryFeature)
	suite.skipIfUnsupported(TransactionsFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	mgr := globalCluster.QueryIndexes()
	err := mgr.CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	suite.Eventually(func() bool {
		res, err := globalCluster.Query(fmt.Sprintf("SELECT 1 FROM %s", globalBucket.Name()), &QueryOptions{
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
	res, err := globalCluster.Query(fmt.Sprintf("INSERT INTO `%s` VALUES (\"%s\", {}) RETURNING meta().id", globalBucket.Name(), docID), &QueryOptions{
		AsTransaction: &SingleQueryTransactionOptions{
			DurabilityLevel: DurabilityLevelMajority,
		},
		Adhoc: true,
	})
	suite.Require().Nil(err, err)

	var something interface{}
	err = res.One(&something)
	suite.Require().NoError(err, err)

	suite.Assert().Equal(map[string]interface{}{"id": docID}, something)

	err = res.Err()
	suite.Require().Nil(err, err)

	meta, err := res.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Equal(uint64(1), meta.Metrics.MutationCount)
}

// We have to manually mock this because testify won't let return something which can iterate.
type mockQueryRowReader struct {
	Dataset []testBreweryDocument
	mockQueryRowReaderBase
}

type mockQueryRowReaderBase struct {
	Meta     []byte
	MetaErr  error
	CloseErr error
	RowsErr  error
	PName    string

	Suite *UnitTestSuite

	idx int
}

func (arr *mockQueryRowReader) NextRow() []byte {
	if arr.idx == len(arr.Dataset) {
		return nil
	}

	idx := arr.idx
	arr.idx++

	return arr.Suite.mustConvertToBytes(arr.Dataset[idx])
}

func (arr *mockQueryRowReaderBase) MetaData() ([]byte, error) {
	return arr.Meta, arr.MetaErr
}

func (arr *mockQueryRowReaderBase) Close() error {
	return arr.CloseErr
}

func (arr *mockQueryRowReaderBase) Err() error {
	return arr.RowsErr
}

func (arr *mockQueryRowReaderBase) PreparedName() (string, error) {
	return arr.PName, nil
}

func (arr *mockQueryRowReaderBase) Endpoint() string {
	return ""
}

func (suite *UnitTestSuite) newMockQueryProvider(prepared bool, reader queryRowReader) (*mockQueryProviderCoreProvider, *mock.Call) {
	queryProvider := new(mockQueryProviderCoreProvider)
	methodName := "N1QLQuery"
	if prepared {
		methodName = "PreparedN1QLQuery"
	}
	call := queryProvider.
		On(methodName, nil, mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(reader, nil).
		Once()

	return queryProvider, call
}

func (suite *UnitTestSuite) queryCluster(prepared bool, retryStrategy *coreRetryStrategyWrapper, reader queryRowReader, runFn func(args mock.Arguments)) *Cluster {
	if retryStrategy == nil {
		retryStrategy = newCoreRetryStrategyWrapper(NewBestEffortRetryStrategy(nil))
	}
	provider, call := suite.newMockQueryProvider(prepared, reader)
	if runFn != nil {
		call.Run(runFn)
	}

	queryProvider := &queryProviderCore{
		provider: provider,
	}

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("getMeter").Return(nil)
	cli.On("MarkOpBeginning").Return()
	cli.On("MarkOpCompleted").Return()

	cluster := suite.newCluster(cli)

	queryProvider.tracer = newTracerWrapper(&NoopTracer{})
	queryProvider.retryStrategyWrapper = retryStrategy
	queryProvider.timeouts.QueryTimeout = 75000 * time.Millisecond

	return cluster
}

func (suite *UnitTestSuite) assertQueryBeerResult(dataset testQueryDataset, result *QueryResult) {
	var breweries []testBreweryDocument
	for result.Next() {
		var doc testBreweryDocument
		err := result.Row(&doc)
		suite.Require().Nil(err, err)
		breweries = append(breweries, doc)
	}

	suite.Assert().Len(breweries, len(dataset.Results))

	err := result.Err()
	suite.Require().Nil(err, err)

	metadata, err := result.MetaData()
	suite.Require().Nil(err, err)

	var aMeta QueryMetaData
	err = aMeta.fromData(dataset.jsonQueryResponse)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(&aMeta, metadata)
}

func (suite *UnitTestSuite) TestQueryAdhoc() {
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
	var cluster *Cluster
	cluster = suite.queryCluster(false, rs, reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.N1QLQueryOptions)
		suite.Assert().Equal(rs, opts.RetryStrategy)
		now := time.Now()
		if opts.Deadline.Before(now.Add(70*time.Second)) || opts.Deadline.After(now.Add(75*time.Second)) {
			suite.Failf("Deadline should have been <75s and >70s but was %s", opts.Deadline.String())
		}

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Contains(actualOptions, "client_context_id")
		suite.Assert().Equal(statement, actualOptions["statement"])
	})

	result, err := cluster.Query(statement, &QueryOptions{
		Adhoc: true,
	})
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	suite.assertQueryBeerResult(dataset, result)
}

func (suite *UnitTestSuite) TestQueryPrepared() {
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
	var cluster *Cluster
	cluster = suite.queryCluster(true, rs, reader, func(args mock.Arguments) {
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
	})

	result, err := cluster.Query(statement, nil)
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	suite.assertQueryBeerResult(dataset, result)
}

func (suite *UnitTestSuite) TestQueryResultsOne() {
	var dataset testQueryDataset
	err := loadJSONTestDataset("beer_sample_query_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockQueryRowReader{
		Dataset: dataset.Results,
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			Meta:  suite.mustConvertToBytes(dataset.jsonQueryResponse),
			Suite: suite,
		},
	}
	result := newQueryResult(reader)

	var doc testBreweryDocument
	err = result.One(&doc)
	suite.Require().Nil(err, err)

	suite.Assert().Equal(dataset.Results[0], doc)

	// Test that One iterated all rows.
	var count int
	for result.Next() {
		count++
	}
	suite.Assert().Zero(count)

	suite.Assert().Nil(result.reader.NextRow())

	err = result.Err()
	suite.Require().Nil(err, err)

	metadata, err := result.MetaData()
	suite.Require().Nil(err, err)

	var aMeta QueryMetaData
	err = aMeta.fromData(dataset.jsonQueryResponse)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(&aMeta, metadata)
}

func (suite *UnitTestSuite) TestQueryResultsErr() {
	reader := &mockQueryRowReader{
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			RowsErr: errors.New("some error"),
			Suite:   suite,
		},
	}
	result := newQueryResult(reader)

	err := result.Err()
	suite.Require().NotNil(err, err)
}

func (suite *UnitTestSuite) TestQueryResultsCloseErr() {
	retErr := errors.New("some error")
	reader := &mockQueryRowReader{
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			CloseErr: retErr,
			Suite:    suite,
		},
	}
	result := newQueryResult(reader)

	err := result.Close()
	suite.Require().Equal(retErr, err)
}

func (suite *UnitTestSuite) TestQueryResultsOneErr() {
	retErr := errors.New("some error")
	var dataset testQueryDataset
	err := loadJSONTestDataset("beer_sample_query_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockQueryRowReader{
		Dataset: dataset.Results,
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			RowsErr: retErr,
			Suite:   suite,
		},
	}
	result := newQueryResult(reader)

	var doc testBreweryDocument
	err = result.One(&doc)
	suite.Require().NoError(err, err)

	err = result.Err()
	suite.Require().Equal(retErr, err)
}

func (suite *UnitTestSuite) TestQueryUntypedError() {
	retErr := errors.New("an error")
	provider := new(mockQueryProviderCoreProvider)
	provider.
		On("N1QLQuery", nil, mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(nil, retErr)

	queryProvider := &queryProviderCore{
		provider: provider,
	}
	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("getMeter").Return(nil)
	cli.On("MarkOpBeginning").Return()
	cli.On("MarkOpCompleted").Return()

	cluster := suite.newCluster(cli)

	queryProvider.tracer = newTracerWrapper(&NoopTracer{})
	queryProvider.retryStrategyWrapper = newCoreRetryStrategyWrapper(NewBestEffortRetryStrategy(nil))
	queryProvider.timeouts.QueryTimeout = 75000 * time.Millisecond

	result, err := cluster.Query("SELECT * FROM dataset", &QueryOptions{
		Adhoc: true,
	})
	suite.Require().Equal(retErr, err)
	suite.Require().Nil(result)
}

func (suite *UnitTestSuite) TestQueryGocbcoreError() {
	retErr := &gocbcore.N1QLError{
		Endpoint:        "http://localhost:8093",
		Statement:       "SELECT * FROM dataset",
		ClientContextID: "context",
		Errors:          []gocbcore.N1QLErrorDesc{{Code: 5000, Message: "Internal Error"}},
	}

	provider := new(mockQueryProviderCoreProvider)
	provider.
		On("N1QLQuery", nil, mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(nil, retErr)

	queryProvider := &queryProviderCore{
		provider: provider,
	}

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("getMeter").Return(nil)
	cli.On("MarkOpBeginning").Return()
	cli.On("MarkOpCompleted").Return()

	cluster := suite.newCluster(cli)
	queryProvider.tracer = newTracerWrapper(&NoopTracer{})
	queryProvider.retryStrategyWrapper = newCoreRetryStrategyWrapper(NewBestEffortRetryStrategy(nil))
	queryProvider.timeouts.QueryTimeout = 75000 * time.Millisecond

	result, err := cluster.Query("SELECT * FROM dataset", &QueryOptions{
		Adhoc: true,
	})
	suite.Require().IsType(&QueryError{}, err)
	suite.Require().Equal(&QueryError{
		Endpoint:        "http://localhost:8093",
		Statement:       "SELECT * FROM dataset",
		ClientContextID: "context",
		Errors:          []QueryErrorDesc{{Code: 5000, Message: "Internal Error"}},
	}, err)
	suite.Require().Nil(result)
}

func (suite *UnitTestSuite) TestQueryTimeoutOption() {
	reader := new(mockQueryRowReader)

	cluster := suite.newCluster(nil)
	statement := "SELECT * FROM dataset"

	rs := newCoreRetryStrategyWrapper(NewBestEffortRetryStrategy(nil))
	provider := new(mockQueryProviderCoreProvider)
	provider.
		On("N1QLQuery", nil, mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Run(func(args mock.Arguments) {
			opts := args.Get(1).(gocbcore.N1QLQueryOptions)
			suite.Assert().Equal(rs, opts.RetryStrategy)
			now := time.Now()
			if opts.Deadline.Before(now.Add(20*time.Second)) || opts.Deadline.After(now.Add(25*time.Second)) {
				suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
			}
		}).
		Return(reader, nil)

	queryProvider := &queryProviderCore{
		provider: provider,
	}
	queryProvider.tracer = newTracerWrapper(&NoopTracer{})
	queryProvider.retryStrategyWrapper = rs
	queryProvider.timeouts.QueryTimeout = 75000 * time.Millisecond

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("getMeter").Return(nil)
	cli.On("MarkOpBeginning").Return()
	cli.On("MarkOpCompleted").Return()

	cluster.connectionManager = cli

	result, err := cluster.Query(statement, &QueryOptions{
		Timeout: 25 * time.Second,
		Adhoc:   true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestQueryNamedParams() {
	reader := new(mockQueryRowReader)

	statement := "SELECT * FROM dataset"
	params := map[string]interface{}{
		"num":     1,
		"imafish": "namedbarry",
		"$cilit":  "bang",
	}

	cluster := suite.queryCluster(false, nil, reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.N1QLQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(float64(1), actualOptions["$num"])
		suite.Assert().Equal("namedbarry", actualOptions["$imafish"])
		suite.Assert().Equal("bang", actualOptions["$cilit"])
	})

	result, err := cluster.Query(statement, &QueryOptions{
		NamedParameters: params,
		Adhoc:           true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestQueryPositionalParams() {
	reader := new(mockQueryRowReader)

	statement := "SELECT * FROM dataset"
	params := []interface{}{float64(1), "imafish"}

	cluster := suite.queryCluster(false, nil, reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.N1QLQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		if suite.Assert().Contains(actualOptions, "args") {
			suite.Require().Equal(params, actualOptions["args"])
		}
	})

	result, err := cluster.Query(statement, &QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestQueryBothPositionalAndNamedParams() {
	reader := new(mockQueryRowReader)

	statement := "SELECT * FROM dataset"
	positionalParams := []interface{}{float64(1), "imafish"}
	namedParams := map[string]interface{}{
		"num":     1,
		"imafish": "namedbarry",
		"$cilit":  "bang",
	}

	cluster := suite.queryCluster(false, nil, reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.N1QLQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		if suite.Assert().Contains(actualOptions, "args") {
			suite.Require().Equal(positionalParams, actualOptions["args"])
		}

		suite.Assert().Equal(float64(1), actualOptions["$num"])
		suite.Assert().Equal("namedbarry", actualOptions["$imafish"])
		suite.Assert().Equal("bang", actualOptions["$cilit"])
	})

	result, err := cluster.Query(statement, &QueryOptions{
		PositionalParameters: positionalParams,
		NamedParameters:      namedParams,
		Adhoc:                true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestQueryClientContextID() {
	reader := new(mockQueryRowReader)

	statement := "SELECT * FROM dataset"
	contextID := "62d29101-0c9f-400d-af2b-9bd44a557a7c"

	cluster := suite.queryCluster(false, nil, reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.N1QLQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(contextID, actualOptions["client_context_id"])
	})

	result, err := cluster.Query(statement, &QueryOptions{
		ClientContextID: contextID,
		Adhoc:           true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestQueryNoMetrics() {
	var dataset testQueryDataset
	err := loadJSONTestDataset("beer_sample_query_dataset_no_metrics", &dataset)
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
	var cluster *Cluster
	cluster = suite.queryCluster(true, rs, reader, func(args mock.Arguments) {
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
	})

	result, err := cluster.Query(statement, nil)
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	suite.assertQueryBeerResult(dataset, result)

	metadata, err := result.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Zero(metadata.Metrics.ElapsedTime)
	suite.Assert().Zero(metadata.Metrics.ErrorCount)
	suite.Assert().Zero(metadata.Metrics.ExecutionTime)
	suite.Assert().Zero(metadata.Metrics.MutationCount)
	suite.Assert().Zero(metadata.Metrics.ResultCount)
	suite.Assert().Zero(metadata.Metrics.ResultSize)
	suite.Assert().Zero(metadata.Metrics.SortCount)
	suite.Assert().Zero(metadata.Metrics.WarningCount)
}

func (suite *UnitTestSuite) TestQueryRaw() {
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

	var cluster *Cluster
	cluster = suite.queryCluster(false, nil, reader, func(args mock.Arguments) {})

	result, err := cluster.Query(statement, &QueryOptions{
		Adhoc: true,
	})
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	raw := result.Raw()

	suite.Assert().False(result.Next())
	suite.Assert().Error(result.One([]string{}))
	suite.Assert().Error(result.Err())
	suite.Assert().Error(result.Close())
	suite.Assert().Error(result.Row([]string{}))

	_, err = result.MetaData()
	suite.Assert().Error(err)

	var i int
	for b := raw.NextBytes(); b != nil; b = raw.NextBytes() {
		suite.Assert().Equal(suite.mustConvertToBytes(dataset.Results[i]), b)
		i++
	}

	err = raw.Err()
	suite.Require().Nil(err, err)

	metadata, err := raw.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Equal(reader.Meta, metadata)
}

func (suite *UnitTestSuite) TestQueryPreserveExpiry() {
	reader := new(mockQueryRowReader)

	statement := "UPDATE default AS d SET d.comment = \"xyz\";"

	cluster := suite.queryCluster(false, nil, reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.N1QLQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(true, actualOptions["preserve_expiry"])
	})

	result, err := cluster.Query(statement, &QueryOptions{
		PreserveExpiry: true,
		Adhoc:          true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}
