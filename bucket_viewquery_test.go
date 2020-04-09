package gocb

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcore/v8"
	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) TestViewQuery() {
	suite.skipIfUnsupported(ViewFeature)
	suite.skipIfUnsupported(ViewIndexUpsertBugFeature)

	n := suite.setupViews()
	suite.runViewsTest(n)
}

func (suite *IntegrationTestSuite) runViewsTest(n int) {
	deadline := time.Now().Add(10 * time.Second)
	var result *ViewResult
	for {
		var err error
		result, err = globalBucket.ViewQuery("ddoc_test", "test", &ViewOptions{
			Timeout:   1 * time.Second,
			Namespace: DesignDocumentNamespaceDevelopment,
			Reduce:    false,
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
	}

	metadata, err := result.MetaData()
	suite.Require().Nil(err, "Metadata had error: %v", err)

	suite.Assert().NotEmpty(metadata.TotalRows)
	suite.Assert().NotEmpty(metadata.Debug)
}

func (suite *IntegrationTestSuite) setupViews() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "views")
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
	cluster := clusterFromOptions(ClusterOptions{})
	b := newBucket(&cluster.sb, "mock")

	provider := new(mockViewProvider)
	provider.
		On("ViewQuery", mock.AnythingOfType("gocbcore.ViewQueryOptions")).
		Run(runFn).
		Return(reader, nil)

	cli := new(mockClient)
	cli.On("getViewProvider").Return(provider, nil)

	b.cacheClient(cli)

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
		opts := args.Get(0).(gocbcore.ViewQueryOptions)
		suite.Assert().Equal(bucket.sb.RetryStrategyWrapper, opts.RetryStrategy)
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
