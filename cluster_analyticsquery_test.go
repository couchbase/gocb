package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v9"

	"github.com/stretchr/testify/mock"
)

type testAnalyticsDataset struct {
	Results []testBreweryDocument
	jsonAnalyticsResponse
}

type analyticsIface interface {
	AnalyticsQuery(string, *AnalyticsOptions) (*AnalyticsResult, error)
}

func (suite *IntegrationTestSuite) TestClusterAnalyticsQuery() {
	suite.skipIfUnsupported(AnalyticsFeature)

	n := suite.setupClusterAnalytics()
	query := fmt.Sprintf("SELECT `testAnalytics`.* FROM `testAnalytics` WHERE service=? LIMIT %d;", n)
	suite.runAnalyticsTest(n, query, "", "", globalCluster)
}

func (suite *IntegrationTestSuite) runAnalyticsTest(n int, query, bucket, scope string, provider analyticsIface) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		suite.tracer.Reset()
		suite.meter.Reset()
		contextID := "contextID"
		result, err := provider.AnalyticsQuery(query, &AnalyticsOptions{
			PositionalParameters: []interface{}{"analytics"},
			ClientContextID:      contextID,
		})
		suite.Require().Nil(err, "Failed to execute query %v", err)

		suite.Require().Contains(suite.tracer.Spans, nil)
		nilParents := suite.tracer.Spans[nil]
		suite.Require().Equal(1, len(nilParents))
		suite.AssertHTTPOpSpan(nilParents[0], "analytics",
			HTTPOpSpanExpectations{
				bucket:                  bucket,
				scope:                   scope,
				statement:               query,
				numDispatchSpans:        1,
				atLeastNumDispatchSpans: false,
				hasEncoding:             true,
				service:                 "analytics",
				dispatchOperationID:     "contextID",
			})

		suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "analytics", "analytics"), 1, false)
		suite.AssertMetrics(makeMetricsKey(meterNameResponses, "analytics", ""), 1, true)

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

func (suite *IntegrationTestSuite) setupClusterAnalytics() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "analytics", "", "")
	suite.Require().Nil(err, "Failed to create dataset %v", err)

	mgr := globalCluster.AnalyticsIndexes()
	err = mgr.CreateDataset("testAnalytics", globalBucket.Name(), &CreateAnalyticsDatasetOptions{
		IgnoreIfExists: true,
		Timeout:        5 * time.Second,
	})
	suite.Require().Nil(err, "Failed to create dataset %v", err)

	err = mgr.ConnectLink(&ConnectAnalyticsLinkOptions{
		Timeout: 10 * time.Second,
	})
	suite.Require().Nil(err, "Failed to connect link %v", err)

	return n
}

// We have to manually mock this because testify won't let return something which can iterate.
type mockAnalyticsRowReader struct {
	Dataset  []testBreweryDocument
	Meta     []byte
	MetaErr  error
	CloseErr error
	RowsErr  error

	Suite *UnitTestSuite

	idx int
}

func (arr *mockAnalyticsRowReader) NextRow() []byte {
	if arr.idx == len(arr.Dataset) {
		return nil
	}

	idx := arr.idx
	arr.idx++

	return arr.Suite.mustConvertToBytes(arr.Dataset[idx])
}

func (arr *mockAnalyticsRowReader) MetaData() ([]byte, error) {
	return arr.Meta, arr.MetaErr
}

func (arr *mockAnalyticsRowReader) Close() error {
	return arr.CloseErr
}

func (arr *mockAnalyticsRowReader) Err() error {
	return arr.RowsErr
}

func (suite *IntegrationTestSuite) TestClusterAnalyticsQueryContext() {
	suite.skipIfUnsupported(AnalyticsFeature)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := globalCluster.AnalyticsQuery("SELECT 1=1", &AnalyticsOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
	defer cancel()

	res, err = globalCluster.AnalyticsQuery("SELECT 1=1", &AnalyticsOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)
}

func (suite *UnitTestSuite) TestAnalyticsQuery() {
	var dataset testAnalyticsDataset
	err := loadJSONTestDataset("beer_sample_analytics_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockAnalyticsRowReader{
		Dataset: dataset.Results,
		Meta:    suite.mustConvertToBytes(dataset.jsonAnalyticsResponse),
		Suite:   suite,
	}

	statement := "SELECT * FROM dataset"

	var cluster *Cluster
	cluster = suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)
		suite.Assert().Equal(0, opts.Priority)
		suite.Assert().Equal(cluster.retryStrategyWrapper, opts.RetryStrategy)
		now := time.Now()
		if opts.Deadline.Before(now.Add(70*time.Second)) || opts.Deadline.After(now.Add(75*time.Second)) {
			suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
		}

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Contains(actualOptions, "statement")
		suite.Assert().Contains(actualOptions, "client_context_id")
		suite.Assert().Equal(statement, actualOptions["statement"])
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, nil)
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	var breweries []testBreweryDocument
	for result.Next() {
		var doc testBreweryDocument
		err := result.Row(&doc)
		suite.Require().Nil(err, err)
		breweries = append(breweries, doc)
	}

	suite.Assert().Len(breweries, len(dataset.Results))

	err = result.Err()
	suite.Require().Nil(err, err)

	metadata, err := result.MetaData()
	suite.Require().Nil(err, err)

	var aMeta AnalyticsMetaData
	err = aMeta.fromData(dataset.jsonAnalyticsResponse)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(&aMeta, metadata)
}

func (suite *UnitTestSuite) TestAnalyticsQueryResultsOne() {
	var dataset testAnalyticsDataset
	err := loadJSONTestDataset("beer_sample_analytics_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockAnalyticsRowReader{
		Dataset: dataset.Results,
		Meta:    suite.mustConvertToBytes(dataset.jsonAnalyticsResponse),
		Suite:   suite,
	}
	result := &AnalyticsResult{
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

	var aMeta AnalyticsMetaData
	err = aMeta.fromData(dataset.jsonAnalyticsResponse)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(&aMeta, metadata)
}

func (suite *UnitTestSuite) TestAnalyticsQueryResultsErr() {
	reader := &mockAnalyticsRowReader{
		RowsErr: errors.New("some error"),
		Suite:   suite,
	}
	result := &AnalyticsResult{
		reader: reader,
	}

	err := result.Err()
	suite.Require().NotNil(err, err)
}

func (suite *UnitTestSuite) TestAnalyticsQueryResultsCloseErr() {
	reader := &mockAnalyticsRowReader{
		CloseErr: errors.New("some error"),
		Suite:    suite,
	}
	result := &AnalyticsResult{
		reader: reader,
	}

	err := result.Close()
	suite.Require().NotNil(err, err)
}

func (suite *UnitTestSuite) TestAnalyticsQueryUntypedError() {
	retErr := errors.New("an error")
	analyticsProvider := new(mockAnalyticsProvider)
	analyticsProvider.
		On("AnalyticsQuery", nil, mock.AnythingOfType("gocbcore.AnalyticsQueryOptions")).
		Return(nil, retErr)

	cli := new(mockConnectionManager)
	cli.On("getAnalyticsProvider").Return(analyticsProvider, nil)

	cluster := suite.newCluster(cli)

	result, err := cluster.AnalyticsQuery("SELECT * FROM dataset", nil)
	suite.Require().Equal(retErr, err)
	suite.Require().Nil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryGocbcoreError() {
	retErr := &gocbcore.AnalyticsError{
		Endpoint:        "http://localhost:8095",
		Statement:       "SELECT * FROM dataset",
		ClientContextID: "context",
		Errors:          []gocbcore.AnalyticsErrorDesc{{Code: 24001, Message: "Compilation error:"}},
	}

	analyticsProvider := new(mockAnalyticsProvider)
	analyticsProvider.
		On("AnalyticsQuery", nil, mock.AnythingOfType("gocbcore.AnalyticsQueryOptions")).
		Return(nil, retErr)

	cli := new(mockConnectionManager)
	cli.On("getAnalyticsProvider").Return(analyticsProvider, nil)

	cluster := suite.newCluster(cli)

	result, err := cluster.AnalyticsQuery("SELECT * FROM dataset", nil)
	suite.Require().IsType(&AnalyticsError{}, err)
	suite.Require().Equal(&AnalyticsError{
		Endpoint:        "http://localhost:8095",
		Statement:       "SELECT * FROM dataset",
		ClientContextID: "context",
		Errors:          []AnalyticsErrorDesc{{Code: 24001, Message: "Compilation error:"}},
	}, err)
	suite.Require().Nil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryPriority() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"

	analyticsProvider := new(mockAnalyticsProvider)
	analyticsProvider.
		On("AnalyticsQuery", nil, mock.AnythingOfType("gocbcore.AnalyticsQueryOptions")).
		Run(func(args mock.Arguments) {
			opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)
			suite.Assert().Equal(-1, opts.Priority)
		}).
		Return(reader, nil)

	cli := new(mockConnectionManager)
	cli.On("getAnalyticsProvider").Return(analyticsProvider, nil)

	cluster := suite.newCluster(cli)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		Priority: true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryTimeoutOption() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"

	var cluster *Cluster
	cluster = suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)
		suite.Assert().Equal(0, opts.Priority)
		suite.Assert().Equal(cluster.retryStrategyWrapper, opts.RetryStrategy)
		now := time.Now()
		if opts.Deadline.Before(now.Add(20*time.Second)) || opts.Deadline.After(now.Add(25*time.Second)) {
			suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
		}
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		Timeout: 25 * time.Second,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryGCCCPUnsupported() {
	retErr := errors.New("an error")
	analyticsProvider := new(mockAnalyticsProvider)
	analyticsProvider.
		On("AnalyticsQuery", nil, mock.AnythingOfType("gocbcore.AnalyticsQueryOptions")).
		Return(nil, retErr)

	cli := new(mockConnectionManager)
	cli.On("getAnalyticsProvider").Return(analyticsProvider, nil)

	cluster := suite.newCluster(cli)

	_, err := cluster.AnalyticsQuery("SELECT * FROM dataset", nil)
	suite.Require().NotNil(err)
}

func (suite *UnitTestSuite) TestAnalyticsQueryNamedParams() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"
	params := map[string]interface{}{
		"num":     1,
		"imafish": "namedbarry",
		"$cilit":  "bang",
	}

	cluster := suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(statement, actualOptions["statement"])
		suite.Assert().NotEmpty(actualOptions["client_context_id"])
		suite.Assert().Equal(float64(1), actualOptions["$num"])
		suite.Assert().Equal("namedbarry", actualOptions["$imafish"])
		suite.Assert().Equal("bang", actualOptions["$cilit"])
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		NamedParameters: params,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryPositionalParams() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"
	params := []interface{}{float64(1), "imafish"}

	cluster := suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(statement, actualOptions["statement"])
		suite.Assert().NotEmpty(actualOptions["client_context_id"])
		if suite.Assert().Contains(actualOptions, "args") {
			suite.Require().Equal(params, actualOptions["args"])
		}
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		PositionalParameters: params,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryBothParams() {
	statement := "SELECT * FROM dataset"
	params := []interface{}{float64(1), "imafish"}
	namedParams := map[string]interface{}{
		"num":     1,
		"imafish": "namedbarry",
		"$cilit":  "bang",
	}

	analyticsProvider := new(mockAnalyticsProvider)

	cli := new(mockConnectionManager)
	cli.On("getAnalyticsProvider").Return(analyticsProvider, nil)

	cluster := suite.newCluster(cli)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		PositionalParameters: params,
		NamedParameters:      namedParams,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected invalid argument error was %s", err)
	}
	suite.Require().Nil(result)
	analyticsProvider.AssertNotCalled(suite.T(), "AnalyticsQuery")
}

func (suite *UnitTestSuite) TestAnalyticsQueryClientContextID() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"
	contextID := "62d29101-0c9f-400d-af2b-9bd44a557a7c"

	cluster := suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(statement, actualOptions["statement"])
		suite.Assert().Equal(contextID, actualOptions["client_context_id"])
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		ClientContextID: contextID,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryRawParam() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"
	params := map[string]interface{}{
		"raw": "param",
	}

	cluster := suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(statement, actualOptions["statement"])
		suite.Assert().NotEmpty(actualOptions["client_context_id"])
		if suite.Assert().Contains(actualOptions, "raw") {
			suite.Require().Equal("param", actualOptions["raw"])
		}
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		Raw: params,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryReadonly() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"

	cluster := suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(statement, actualOptions["statement"])
		suite.Assert().NotEmpty(actualOptions["client_context_id"])
		suite.Assert().Equal(true, actualOptions["readonly"])
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		Readonly: true,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryConsistencyNotBounded() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"

	cluster := suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(statement, actualOptions["statement"])
		suite.Assert().NotEmpty(actualOptions["client_context_id"])
		suite.Assert().Equal("not_bounded", actualOptions["scan_consistency"])
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		ScanConsistency: AnalyticsScanConsistencyNotBounded,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryConsistencyRequestPlus() {
	reader := new(mockAnalyticsRowReader)

	statement := "SELECT * FROM dataset"

	cluster := suite.analyticsCluster(nil, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.AnalyticsQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Equal(statement, actualOptions["statement"])
		suite.Assert().NotEmpty(actualOptions["client_context_id"])
		suite.Assert().Equal("request_plus", actualOptions["scan_consistency"])
	}, reader)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		ScanConsistency: AnalyticsScanConsistencyRequestPlus,
	})
	suite.Require().Nil(err)
	suite.Require().NotNil(result)
}

func (suite *UnitTestSuite) TestAnalyticsQueryConsistencyInvalid() {
	statement := "SELECT * FROM dataset"

	analyticsProvider := new(mockAnalyticsProvider)

	cli := new(mockConnectionManager)
	cli.On("getAnalyticsProvider").Return(analyticsProvider, nil)

	cluster := suite.newCluster(cli)

	result, err := cluster.AnalyticsQuery(statement, &AnalyticsOptions{
		ScanConsistency: 5,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected invalid argument error was %s", err)
	}
	suite.Require().Nil(result)
	analyticsProvider.AssertNotCalled(suite.T(), "AnalyticsQuery")
}

func (suite *UnitTestSuite) analyticsCluster(ctx context.Context, runFn func(args mock.Arguments), reader analyticsRowReader) *Cluster {
	analyticsProvider := new(mockAnalyticsProvider)
	analyticsProvider.
		On("AnalyticsQuery", ctx, mock.AnythingOfType("gocbcore.AnalyticsQueryOptions")).
		Run(runFn).
		Return(reader, nil)

	cli := new(mockConnectionManager)
	cli.On("getAnalyticsProvider").Return(analyticsProvider, nil)

	cluster := suite.newCluster(cli)

	return cluster
}

func (suite *UnitTestSuite) TestAnalyticsQueryRaw() {
	var dataset testAnalyticsDataset
	err := loadJSONTestDataset("beer_sample_analytics_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockAnalyticsRowReader{
		Dataset: dataset.Results,
		Meta:    suite.mustConvertToBytes(dataset.jsonAnalyticsResponse),
		Suite:   suite,
	}

	statement := "SELECT * FROM dataset"

	var cluster *Cluster
	cluster = suite.analyticsCluster(nil, func(args mock.Arguments) {}, reader)

	result, err := cluster.AnalyticsQuery(statement, nil)
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
