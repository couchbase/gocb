package gocb

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v9"

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

	_, err = globalScope.Query(fmt.Sprintf("CREATE PRIMARY INDEX ON `%s`", globalCollection.Name()), nil)
	suite.Require().Nil(err, "Failed to create index %v", err)

	return n
}

func (suite *IntegrationTestSuite) runScopePreparedQueryPositionalTest(n int) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalCollection.Name(), globalCollection.Name(), n)
	suite.runPreparedQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, []interface{}{"scopequery"})
}

func (suite *IntegrationTestSuite) runScopePreparedQueryNamedTest(n int) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service LIMIT %d;", globalCollection.Name(), globalCollection.Name(), n)
	suite.runPreparedQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, map[string]interface{}{"service": "scopequery"})
}

func (suite *IntegrationTestSuite) runScopeQueryPositionalTest(n int, withMetrics bool) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=? LIMIT %d;", globalCollection.Name(), globalCollection.Name(), n)
	suite.runQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, withMetrics, []interface{}{"scopequery"})
}

func (suite *IntegrationTestSuite) runScopeQueryNamedTest(n int, withMetrics bool) {
	query := fmt.Sprintf("SELECT `%s`.* FROM `%s` WHERE service=$service LIMIT %d;", globalCollection.Name(), globalCollection.Name(), n)
	suite.runQueryTest(n, query, globalBucket.Name(), globalScope.Name(), globalScope, withMetrics, map[string]interface{}{"service": "scopequery"})
}

func (suite *UnitTestSuite) queryScope(prepared bool, reader queryRowReader, runFn func(args mock.Arguments)) *Scope {
	queryProvider, call := suite.newMockQueryProvider(prepared, reader)
	call.Run(runFn)

	cli := new(mockConnectionManager)
	cli.On("getQueryProvider").Return(queryProvider, nil)

	b := suite.bucket("queryBucket", TimeoutsConfig{QueryTimeout: 75 * time.Second}, cli)

	scope := suite.newScope(b, "queryScope")

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

	var scope *Scope
	scope = suite.queryScope(true, reader, func(args mock.Arguments) {
		opts := args.Get(0).(gocbcore.N1QLQueryOptions)
		suite.Assert().Equal(scope.retryStrategyWrapper, opts.RetryStrategy)
		now := time.Now()
		if opts.Deadline.Before(now.Add(70*time.Second)) || opts.Deadline.After(now.Add(75*time.Second)) {
			suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
		}

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().Contains(actualOptions, "client_context_id")
		suite.Assert().Equal(actualOptions["query_context"], "queryBucket.queryScope")
	})

	result, err := scope.Query(statement, nil)
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	suite.assertQueryBeerResult(dataset, result)
}
