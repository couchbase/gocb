package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) TestViewQuery() {
	suite.skipIfUnsupported(ViewFeature)
	suite.skipIfUnsupported(ViewIndexUpsertBugFeature)

	n := suite.setupViews()
	suite.runViewsTest(n)
}

func (suite *IntegrationTestSuite) runViewsTest(n int) {
	deadline := time.Now().Add(60 * time.Second)
	var result *ViewResult
	for {
		globalTracer.Reset()
		globalMeter.Reset()
		var err error
		result, err = globalBucket.ViewQuery("ddoc_test", "test", &ViewOptions{
			Timeout:   1 * time.Second,
			Namespace: DesignDocumentNamespaceDevelopment,
			Debug:     true,
		})
		if err != nil {
			sleepDeadline := time.Now().Add(1000 * time.Millisecond)
			if sleepDeadline.After(deadline) {
				sleepDeadline = deadline
			}
			time.Sleep(sleepDeadline.Sub(time.Now()))

			if sleepDeadline == deadline {
				suite.T().Errorf("timed out waiting for indexing")
				return
			}
			continue
		}

		samples := make(map[string]testBreweryDocument)
		for result.Next() {
			row := result.Row()
			suite.Assert().NotEmpty(row.ID)

			var val testBreweryDocument
			err := row.Value(&val)
			suite.Require().Nil(err, err)
			suite.Assert().NotNil(val)

			var key string
			err = row.Key(&key)
			suite.Require().Nil(err, err)
			suite.Assert().NotEmpty(key)

			samples[key] = val
		}

		if len(samples) == n {
			break
		}

		err = result.Err()
		suite.Require().Nil(err, "Result had error %v", err)

		sleepDeadline := time.Now().Add(1000 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))

		if sleepDeadline == deadline {
			suite.T().Errorf("timed out waiting for indexing")
			return
		}

		suite.Require().Contains(globalTracer.GetSpans(), nil)
		nilParents := globalTracer.GetSpans()[nil]
		suite.Require().Equal(1, len(nilParents))
		suite.AssertHTTPOpSpan(nilParents[0], "views",
			HTTPOpSpanExpectations{
				bucket:                  globalConfig.Bucket,
				operationID:             "dev_ddoc_test/test",
				numDispatchSpans:        1,
				atLeastNumDispatchSpans: false,
				hasEncoding:             false,
				dispatchOperationID:     "",
				service:                 "views",
			})

		suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "views", "views"), 1, false)
	}

	metadata, err := result.MetaData()
	suite.Require().Nil(err, "Metadata had error: %v", err)

	suite.Assert().NotEmpty(metadata.TotalRows)
	suite.Assert().NotEmpty(metadata.Debug)
}

func (suite *IntegrationTestSuite) setupViews() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "views", "", "")
	suite.Require().Nil(err, err)

	mgr := globalBucket.ViewIndexes()
	err = mgr.UpsertDesignDocument(DesignDocument{
		Name: "ddoc_test",
		Views: map[string]View{
			"test": {
				Map: `
function (doc, meta) {
  if (doc.service === "views") {
  	emit(meta.id, doc);
  }
}`,
				Reduce: "_count",
			},
		},
	}, DesignDocumentNamespaceDevelopment, &UpsertDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	return n
}

func (suite *IntegrationTestSuite) TestViewQueryContext() {
	suite.skipIfUnsupported(SearchFeature)
	suite.skipIfServerVersionEquals(srvVer750)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := globalBucket.ViewQuery("test", "test", &ViewOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
	defer cancel()

	res, err = globalBucket.ViewQuery("test", "test", &ViewOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)
}

// We have to manually mock this because testify won't let return something which can iterate.
type mockViewRowReader struct {
	Dataset  []jsonViewRow
	Meta     []byte
	MetaErr  error
	CloseErr error
	RowsErr  error

	Suite *UnitTestSuite

	idx int
}

func (arr *mockViewRowReader) NextRow() []byte {
	if arr.idx == len(arr.Dataset) {
		return nil
	}

	idx := arr.idx
	arr.idx++

	return arr.Suite.mustConvertToBytes(arr.Dataset[idx])
}

func (arr *mockViewRowReader) MetaData() ([]byte, error) {
	return arr.Meta, arr.MetaErr
}

func (arr *mockViewRowReader) Close() error {
	return arr.CloseErr
}

func (arr *mockViewRowReader) Err() error {
	return arr.RowsErr
}

type testViewDataset struct {
	Rows []jsonViewRow
	jsonViewResponse
}

func (suite *UnitTestSuite) viewsBucket(reader viewRowReader, runFn func(args mock.Arguments)) *Bucket {
	provider := new(mockViewProvider)
	provider.
		On("ViewQuery", nil, mock.AnythingOfType("gocbcore.ViewQueryOptions")).
		Run(runFn).
		Return(reader, nil)

	cli := new(mockConnectionManager)
	cli.On("getViewProvider", "mockBucket").Return(provider, nil)

	cluster := suite.newCluster(cli)
	b := newBucket(cluster, "mockBucket")

	return b
}

func (suite *UnitTestSuite) TestViewQuery() {
	var dataset testViewDataset
	err := loadJSONTestDataset("beer_sample_views_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockViewRowReader{
		Dataset: dataset.Rows,
		Meta:    suite.mustConvertToBytes(dataset.jsonViewResponse),
		Suite:   suite,
	}

	var bucket *Bucket
	bucket = suite.viewsBucket(reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.ViewQueryOptions)
		suite.Assert().Equal(bucket.retryStrategyWrapper, opts.RetryStrategy)
		now := time.Now()
		if opts.Deadline.Before(now.Add(70*time.Second)) || opts.Deadline.After(now.Add(75*time.Second)) {
			suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
		}

		suite.Assert().Equal("dev_ddoc", opts.DesignDocumentName)
		suite.Assert().Equal("view", opts.ViewName)
		suite.Assert().Equal("_view", opts.ViewType)
		suite.Assert().Equal("56", opts.Options.Get("skip"))
		suite.Assert().Equal("10", opts.Options.Get("limit"))
		suite.Assert().Equal("true", opts.Options.Get("debug"))
	})

	result, err := bucket.ViewQuery("ddoc", "view", &ViewOptions{
		Namespace: DesignDocumentNamespaceDevelopment,
		Skip:      56,
		Limit:     10,
		Debug:     true,
	})
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	expectedBreweries := make(map[string]testBreweryDocument)
	for _, brewery := range dataset.Rows {
		var key string
		suite.Require().Nil(json.Unmarshal(brewery.Key, &key))
		var val testBreweryDocument
		suite.Require().Nil(json.Unmarshal(brewery.Value, &val))
		expectedBreweries[key] = val
	}

	actualBreweries := make(map[string]testBreweryDocument)
	for result.Next() {
		row := result.Row()

		var key string
		suite.Require().Nil(row.Key(&key))
		var val testBreweryDocument
		suite.Require().Nil(row.Value(&val))
		actualBreweries[key] = val
	}

	suite.Assert().Equal(expectedBreweries, actualBreweries)

	meta, err := result.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Equal(dataset.TotalRows, meta.TotalRows)
	suite.Assert().Equal(dataset.DebugInfo, meta.Debug)
}

func (suite *UnitTestSuite) TestViewQueryRaw() {
	var dataset testViewDataset
	err := loadJSONTestDataset("beer_sample_views_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockViewRowReader{
		Dataset: dataset.Rows,
		Meta:    suite.mustConvertToBytes(dataset.jsonViewResponse),
		Suite:   suite,
	}

	var bucket *Bucket
	bucket = suite.viewsBucket(reader, func(args mock.Arguments) {})

	result, err := bucket.ViewQuery("ddoc", "view", &ViewOptions{
		Namespace: DesignDocumentNamespaceDevelopment,
		Skip:      56,
		Limit:     10,
		Debug:     true,
	})
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	raw := result.Raw()

	suite.Assert().False(result.Next())
	suite.Assert().Error(result.Err())
	suite.Assert().Error(result.Close())
	suite.Assert().Zero(result.Row())

	_, err = result.MetaData()
	suite.Assert().Error(err)

	var i int
	for b := raw.NextBytes(); b != nil; b = raw.NextBytes() {
		suite.Assert().Equal(suite.mustConvertToBytes(dataset.Rows[i]), b)
		i++
	}

	err = raw.Err()
	suite.Require().Nil(err, err)

	metadata, err := raw.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Equal(reader.Meta, metadata)
}
