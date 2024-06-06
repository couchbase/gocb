package gocb

import (
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/couchbase/gocb/v2/search"
	"github.com/couchbase/gocbcore/v10"
)

func (suite *IntegrationTestSuite) TestScopeSearch() {
	suite.skipIfUnsupported(SearchFeature)
	suite.skipIfUnsupported(ScopeSearchIndexFeature)
	suite.skipIfUnsupported(ScopeSearchFeature)

	n := suite.setupScopeSearch()
	defer globalScope.SearchIndexes().DropIndex("scope_search_test_index", nil)

	suite.runScopeSearchTest(n)
}

func (suite *IntegrationTestSuite) runScopeSearchTest(n int) {
	deadline := time.Now().Add(60 * time.Second)
	request := SearchRequest{
		SearchQuery: search.NewTermQuery("search").Field("service"),
	}
	var result *SearchResult
	var rows []SearchRow
	for {
		globalTracer.Reset()
		globalMeter.Reset()
		var err error
		result, err = globalScope.Search("scope_search_test_index", request, &SearchOptions{
			Timeout: 1 * time.Second,
			Facets: map[string]search.Facet{
				"type":    search.NewTermFacet("country", 5),
				"date":    search.NewDateFacet("updated", 5).AddRange("updated", "2000-07-22 20:00:20", "2020-07-22 20:00:20"),
				"numeric": search.NewNumericFacet("geo.lat", 5).AddRange("lat", 30, 31),
			},
			IncludeLocations: true,
		})

		suite.Require().Contains(globalTracer.GetSpans(), nil)
		nilParents := globalTracer.GetSpans()[nil]
		suite.Require().Equal(1, len(nilParents))
		suite.AssertHTTPOpSpan(nilParents[0], "search",
			HTTPOpSpanExpectations{
				bucket:                  globalScope.BucketName(),
				scope:                   globalScope.Name(),
				operationID:             "scope_search_test_index",
				numDispatchSpans:        1,
				atLeastNumDispatchSpans: false,
				hasEncoding:             !globalCluster.IsProtostellar(),
				service:                 "search",
			})

		suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "search", "search"), 1, false)

		if err != nil {
			suite.T().Logf("Search failed %s", err)
			sleepDeadline := time.Now().Add(1000 * time.Millisecond)
			if sleepDeadline.After(deadline) {
				sleepDeadline = deadline
			}
			time.Sleep(sleepDeadline.Sub(time.Now()))

			if sleepDeadline == deadline {
				suite.T().Fatalf("timed out waiting for indexing")
			}
			continue
		}

		var thisRows []SearchRow
		for result.Next() {
			row := result.Row()
			thisRows = append(thisRows, row)
		}

		err = result.Err()
		suite.Require().Nil(err, err)

		if n == len(thisRows) {
			rows = thisRows
			break
		}

		sleepDeadline := time.Now().Add(1000 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))

		if sleepDeadline == deadline {
			suite.T().Fatalf("timed out waiting for indexing")
		}
	}

	for _, row := range rows {
		if suite.Assert().Contains(row.Locations, "service") {
			if suite.Assert().Contains(row.Locations["service"], "search") {
				if suite.Assert().NotZero(row.Locations["service"]["search"]) {
					suite.Assert().Zero(row.Locations["service"]["search"][0].Start)
					suite.Assert().NotZero(row.Locations["service"]["search"][0].End)
					suite.Assert().Nil(row.Locations["service"]["search"][0].ArrayPositions)
				}
			}
		}
	}

	metadata, err := result.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().NotEmpty(metadata.Metrics.TotalRows)
	suite.Assert().NotEmpty(metadata.Metrics.Took)
	suite.Assert().NotEmpty(metadata.Metrics.MaxScore)

	facets, err := result.Facets()
	suite.Require().Nil(err, err)
	if suite.Assert().Contains(facets, "type") {
		f := facets["type"]
		suite.Assert().Equal("country", f.Field)
		suite.Assert().Equal(uint64(7), f.Total)
		suite.Assert().Equal(4, len(f.Terms))
		for _, term := range f.Terms {
			switch term.Term {
			case "belgium":
				suite.Assert().Equal(2, term.Count)
			case "states":
				suite.Assert().Equal(2, term.Count)
			case "united":
				suite.Assert().Equal(2, term.Count)
			case "norway":
				suite.Assert().Equal(1, term.Count)
			default:
				suite.Failf("Unexpected facet term %s", term.Term)
			}
		}
	}

	if suite.Assert().Contains(facets, "date") {
		f := facets["date"]
		suite.Assert().Equal(uint64(5), f.Total)
		suite.Assert().Equal("updated", f.Field)
		suite.Assert().Equal(1, len(f.DateRanges))
		suite.Assert().Equal(5, f.DateRanges[0].Count)
		suite.Assert().Equal("2000-07-22T20:00:20Z", f.DateRanges[0].Start)
		suite.Assert().Equal("2020-07-22T20:00:20Z", f.DateRanges[0].End)
		suite.Assert().Equal("updated", f.DateRanges[0].Name)
	}

	if suite.Assert().Contains(facets, "numeric") {
		f := facets["numeric"]
		suite.Assert().Equal(uint64(1), f.Total)
		suite.Assert().Equal("geo.lat", f.Field)
		suite.Assert().Equal(1, len(f.NumericRanges))
		suite.Assert().Equal(1, f.NumericRanges[0].Count)
		suite.Assert().Equal(float64(30), f.NumericRanges[0].Min)
		suite.Assert().Equal(float64(31), f.NumericRanges[0].Max)
		suite.Assert().Equal("lat", f.NumericRanges[0].Name)
	}
}

func (suite *IntegrationTestSuite) setupScopeSearch() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "search", globalScope.scopeName, "")
	suite.Require().Nil(err, err)

	mgr := globalScope.SearchIndexes()
	err = mgr.UpsertIndex(SearchIndex{
		Name:       "scope_search_test_index",
		SourceName: globalBucket.Name(),
		SourceType: "couchbase",
		Type:       "fulltext-index",
	}, nil)
	suite.Require().Nil(err, err)
	return n
}

func (suite *IntegrationTestSuite) TestScopeSearchFeatureNotAvailable() {
	suite.skipIfUnsupported(SearchFeature)
	suite.skipIfSupported(ScopeSearchFeature)

	request := SearchRequest{
		SearchQuery: search.NewTermQuery("search").Field("service"),
	}
	res, err := globalScope.Search("foo", request, nil)

	suite.Assert().ErrorIs(err, ErrFeatureNotAvailable)
	suite.Assert().Nil(res)
}

func (suite *UnitTestSuite) searchScope(reader searchRowReader, runFn func(args mock.Arguments)) *Scope {
	provider := new(mockSearchProviderCoreProvider)
	provider.
		On("SearchQuery", nil, mock.AnythingOfType("gocbcore.SearchQueryOptions")).
		Run(runFn).
		Return(reader, nil)

	searchProvider := &searchProviderCore{
		provider: provider,
		tracer:   &NoopTracer{},
		meter:    newMeterWrapper(&NoopMeter{}),
	}
	cli := new(mockConnectionManager)
	cli.On("getSearchProvider").Return(searchProvider, nil)
	cli.On("MarkOpBeginning").Return()
	cli.On("MarkOpCompleted").Return()

	bucket := suite.bucket("searchBucket", TimeoutsConfig{SearchTimeout: 75 * time.Second}, cli)
	scope := suite.newScope(bucket, "searchScope")

	searchProvider.retryStrategyWrapper = scope.retryStrategyWrapper
	searchProvider.timeouts = scope.timeoutsConfig

	return scope
}

func (suite *UnitTestSuite) TestScopeSearchSetsBucketAndScopeNames() {
	reader := &mockSearchRowReader{
		Dataset: []jsonSearchRow{},
		Meta:    []byte{},
		Suite:   suite,
	}

	request := SearchRequest{
		SearchQuery: search.NewMatchAllQuery(),
	}

	var scope *Scope
	scope = suite.searchScope(reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.SearchQueryOptions)

		suite.Assert().Equal(scope.scopeName, opts.ScopeName)
		suite.Assert().Equal(scope.bucket.bucketName, opts.BucketName)
	})

	_, err := scope.Search("testindex", request, nil)
	suite.Require().Nil(err, err)
}
