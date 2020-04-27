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

func (suite *IntegrationTestSuite) TestQuery() {
	suite.skipIfUnsupported(QueryFeature)

	n := suite.setupQuery()
	suite.runQueryTest(n)
	suite.runPreparedQueryTest(n)
}

func (suite *IntegrationTestSuite) runPreparedQueryTest(n int) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
		result, err := globalCluster.Query(query, &QueryOptions{
			PositionalParameters: []interface{}{"query"},
			Timeout:              1 * time.Second,
		})
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

func (suite *IntegrationTestSuite) runQueryTest(n int) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalBucket.Name(), globalBucket.Name(), n)
		result, err := globalCluster.Query(query, &QueryOptions{
			PositionalParameters: []interface{}{"query"},
			Timeout:              1 * time.Second,
			Adhoc:                true,
		})
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

func (suite *IntegrationTestSuite) setupQuery() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "query")
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

type mockPreparedQueryRowReader struct {
	Dataset []jsonQueryPrepData
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

func (arr *mockPreparedQueryRowReader) NextRow() []byte {
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
	cluster := suite.newCluster()

	queryProvider, call := suite.newMockQueryProvider(prepared, reader)
	call.Run(runFn)

	cli := new(mockClient)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("supportsGCCCP").Return(true)

	cluster.clusterClient = cli

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
		suite.Assert().Equal(cluster.sb.RetryStrategyWrapper, opts.RetryStrategy)
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
		suite.Assert().Equal(cluster.sb.RetryStrategyWrapper, opts.RetryStrategy)
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

	cli := new(mockClient)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("supportsGCCCP").Return(true)

	cluster := suite.newCluster()
	cluster.clusterClient = cli

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

	cli := new(mockClient)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("supportsGCCCP").Return(true)

	cluster := suite.newCluster()
	cluster.clusterClient = cli

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

	cluster := suite.newCluster()
	statement := "SELECT * FROM dataset"

	queryProvider := new(mockQueryProvider)
	queryProvider.
		On("N1QLQuery", mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Run(func(args mock.Arguments) {
			opts := args.Get(0).(gocbcore.N1QLQueryOptions)
			suite.Assert().Equal(cluster.sb.RetryStrategyWrapper, opts.RetryStrategy)
			now := time.Now()
			if opts.Deadline.Before(now.Add(20*time.Second)) || opts.Deadline.After(now.Add(25*time.Second)) {
				suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
			}
		}).
		Return(reader, nil)

	cli := new(mockClient)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("supportsGCCCP").Return(true)

	cluster.clusterClient = cli

	result, err := cluster.Query(statement, &QueryOptions{
		Timeout: 25 * time.Second,
		Adhoc:   true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestQueryGCCCPUnsupported() {
	retErr := errors.New("an error")
	queryProvider := new(mockQueryProvider)
	queryProvider.
		On("N1QLQuery", mock.AnythingOfType("gocbcore.N1QLQueryOptions")).
		Return(nil, retErr)

	cli := new(mockClient)
	cli.On("getQueryProvider").Return(queryProvider, nil)
	cli.On("supportsGCCCP").Return(false)

	cluster := suite.newCluster()
	cluster.clusterClient = cli

	_, err := cluster.Query("SELECT * FROM dataset", nil)
	suite.Require().NotNil(err)
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
