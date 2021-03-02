package gocb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v9"

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

	n := suite.setupClusterQuery()
	suite.Run("TestClusterQuery", func() {
		suite.runClusterQueryPositionalTest(n, true)
		suite.runClusterQueryNamedTest(n, true)
	})
	suite.Run("TestClusterQueryNoMetrics", func() {
		suite.runClusterQueryPositionalTest(n, false)
		suite.runClusterQueryNamedTest(n, false)
	})
	suite.Run("TestClusterPreparedQuery", func() {
		suite.runClusterPreparedQueryPositionalTest(n)
		suite.runClusterPreparedQueryNamedTest(n)
	})
}

func (suite *IntegrationTestSuite) runPreparedQueryTest(n int, query string, queryFn queryIface, params interface{}) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		opts := &QueryOptions{
			Timeout: 5 * time.Second,
		}
		switch p := params.(type) {
		case []interface{}:
			opts.PositionalParameters = p
		case map[string]interface{}:
			opts.NamedParameters = p
		}
		result, err := queryFn.Query(query, opts)
		suite.Require().Nil(err, "Failed to execute query %v", err)

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
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runPreparedQueryTest(n, query, globalCluster, []interface{}{"query"})
}

func (suite *IntegrationTestSuite) runClusterPreparedQueryNamedTest(n int) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runPreparedQueryTest(n, query, globalCluster, map[string]interface{}{"service": "query"})
}

func (suite *IntegrationTestSuite) runQueryTest(n int, query string, queryFn queryIface, withMetrics bool, params interface{}) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		opts := &QueryOptions{
			Timeout: 5 * time.Second,
			Adhoc:   true,
			Metrics: withMetrics,
		}
		switch p := params.(type) {
		case []interface{}:
			opts.PositionalParameters = p
		case map[string]interface{}:
			opts.NamedParameters = p
		}
		result, err := queryFn.Query(query, opts)
		suite.Require().Nil(err, "Failed to execute query %v", err)

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
			suite.Assert().NotZero(metadata.Metrics.ResultCount)
			suite.Assert().NotZero(metadata.Metrics.ResultSize)
		}

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

func (suite *IntegrationTestSuite) runClusterQueryPositionalTest(n int, withMetrics bool) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runQueryTest(n, query, globalCluster, withMetrics, []interface{}{"query"})
}

func (suite *IntegrationTestSuite) runClusterQueryNamedTest(n int, withMetrics bool) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
	suite.runQueryTest(n, query, globalCluster, withMetrics, map[string]interface{}{"service": "query"})
}

func (suite *IntegrationTestSuite) setupClusterQuery() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "query", "", "")
	suite.Require().Nil(err, "Failed to create dataset %v", err)

	mgr := globalCluster.QueryIndexes()
	err = mgr.CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
		Timeout:        30 * time.Second,
	})
	suite.Require().Nil(err, "Failed to create index %v", err)

	return n
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

func (suite *UnitTestSuite) newMockQueryProvider(prepared bool, reader queryRowReader) (*mockQueryProvider, *mock.Call) {
	queryProvider := new(mockQueryProvider)
	methodName := "N1QLQuery"
	if prepared {
		methodName = "PreparedN1QLQuery"
	}
	call := queryProvider.
		On(methodName, mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(reader, nil).
		Once()

	return queryProvider, call
}

func (suite *UnitTestSuite) queryCluster(prepared bool, reader queryRowReader, runFn func(args mock.Arguments)) *Cluster {
	queryProvider, call := suite.newMockQueryProvider(prepared, reader)
	if runFn != nil {
		call.Run(runFn)
	}

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)

	cluster := suite.newCluster(cli)

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

	var cluster *Cluster
	cluster = suite.queryCluster(false, reader, func(args mock.Arguments) {
		opts := args.Get(0).(gocbcore.N1QLQueryOptions)
		suite.Assert().Equal(cluster.retryStrategyWrapper, opts.RetryStrategy)
		now := time.Now()
		if opts.Deadline.Before(now.Add(70*time.Second)) || opts.Deadline.After(now.Add(75*time.Second)) {
			suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
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

	var cluster *Cluster
	cluster = suite.queryCluster(true, reader, func(args mock.Arguments) {
		opts := args.Get(0).(gocbcore.N1QLQueryOptions)
		suite.Assert().Equal(cluster.retryStrategyWrapper, opts.RetryStrategy)
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
	result := &QueryResult{
		reader: reader,
	}

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
	result := &QueryResult{
		reader: reader,
	}

	err := result.Err()
	suite.Require().NotNil(err, err)
}

func (suite *UnitTestSuite) TestQueryResultsCloseErr() {
	reader := &mockQueryRowReader{
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			CloseErr: errors.New("some error"),
			Suite:    suite,
		},
	}
	result := &QueryResult{
		reader: reader,
	}

	err := result.Close()
	suite.Require().NotNil(err, err)
}

func (suite *UnitTestSuite) TestQueryUntypedError() {
	retErr := errors.New("an error")
	queryProvider := new(mockQueryProvider)
	queryProvider.
		On("N1QLQuery", mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(nil, retErr)

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)

	cluster := suite.newCluster(cli)

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

	queryProvider := new(mockQueryProvider)
	queryProvider.
		On("N1QLQuery", mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(nil, retErr)

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)

	cluster := suite.newCluster(cli)

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

	queryProvider := new(mockQueryProvider)
	queryProvider.
		On("N1QLQuery", mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Run(func(args mock.Arguments) {
			opts := args.Get(0).(gocbcore.N1QLQueryOptions)
			suite.Assert().Equal(cluster.retryStrategyWrapper, opts.RetryStrategy)
			now := time.Now()
			if opts.Deadline.Before(now.Add(20*time.Second)) || opts.Deadline.After(now.Add(25*time.Second)) {
				suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
			}
		}).
		Return(reader, nil)

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)

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

	cluster := suite.queryCluster(false, reader, func(args mock.Arguments) {
		opts := args.Get(0).(gocbcore.N1QLQueryOptions)

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

	cluster := suite.queryCluster(false, reader, func(args mock.Arguments) {
		opts := args.Get(0).(gocbcore.N1QLQueryOptions)

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

func (suite *UnitTestSuite) TestQueryClientContextID() {
	reader := new(mockQueryRowReader)

	statement := "SELECT * FROM dataset"
	contextID := "62d29101-0c9f-400d-af2b-9bd44a557a7c"

	cluster := suite.queryCluster(false, reader, func(args mock.Arguments) {
		opts := args.Get(0).(gocbcore.N1QLQueryOptions)

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

	var cluster *Cluster
	cluster = suite.queryCluster(true, reader, func(args mock.Arguments) {
		opts := args.Get(0).(gocbcore.N1QLQueryOptions)
		suite.Assert().Equal(cluster.retryStrategyWrapper, opts.RetryStrategy)
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
	cluster = suite.queryCluster(false, reader, func(args mock.Arguments) {})

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
