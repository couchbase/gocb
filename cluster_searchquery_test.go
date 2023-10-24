package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/mock"

	"github.com/couchbase/gocb/v2/search"
)

func (suite *IntegrationTestSuite) TestSearch() {
	suite.skipIfUnsupported(SearchFeature)

	n := suite.setupSearch()
	defer globalCluster.SearchIndexes().DropIndex("search_test_index", nil)

	suite.runSearchTest(n)
}

func (suite *IntegrationTestSuite) runSearchTest(n int) {
	deadline := time.Now().Add(60 * time.Second)
	query := search.NewTermQuery("search").Field("service")
	var result *SearchResult
	var rows []SearchRow
	for {
		globalTracer.Reset()
		globalMeter.Reset()
		var err error
		result, err = globalCluster.SearchQuery("search_test_index", query, &SearchOptions{
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
				operationID:             "search_test_index",
				numDispatchSpans:        1,
				atLeastNumDispatchSpans: false,
				hasEncoding:             !globalCluster.IsProtostellar(),
				service:                 "search",
			})

		suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "search", "search"), 1, false)

		if err != nil {
			suite.T().Logf("Query failed %s", err)
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

func (suite *IntegrationTestSuite) setupSearch() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "search", "", "")
	suite.Require().Nil(err, err)

	mgr := globalCluster.SearchIndexes()
	err = mgr.UpsertIndex(SearchIndex{
		Name:       "search_test_index",
		SourceName: globalBucket.Name(),
		SourceType: "couchbase",
		Type:       "fulltext-index",
	}, nil)
	suite.Require().Nil(err, err)

	return n
}

func (suite *IntegrationTestSuite) TestSearchContext() {
	suite.skipIfUnsupported(SearchFeature)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := globalCluster.SearchQuery("test", search.NewMatchAllQuery(), &SearchOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
	defer cancel()

	res, err = globalCluster.SearchQuery("test", search.NewMatchAllQuery(), &SearchOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) && !errors.Is(err, ErrTimeout) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)
}

// We have to manually mock this because testify won't let return something which can iterate.
type mockSearchRowReader struct {
	Dataset  []jsonSearchRow
	Meta     []byte
	MetaErr  error
	CloseErr error
	RowsErr  error

	Suite *UnitTestSuite

	idx int
}

func (arr *mockSearchRowReader) NextRow() []byte {
	if arr.idx == len(arr.Dataset) {
		return nil
	}

	idx := arr.idx
	arr.idx++

	return arr.Suite.mustConvertToBytes(arr.Dataset[idx])
}

func (arr *mockSearchRowReader) MetaData() ([]byte, error) {
	return arr.Meta, arr.MetaErr
}

func (arr *mockSearchRowReader) Close() error {
	return arr.CloseErr
}

func (arr *mockSearchRowReader) Err() error {
	return arr.RowsErr
}

type testSearchDataset struct {
	Hits []jsonSearchRow
	jsonSearchResponse
}

func (suite *UnitTestSuite) searchCluster(reader searchRowReader, runFn func(args mock.Arguments)) *Cluster {
	provider := new(mockSearchProviderCoreProvider)
	provider.
		On("SearchQuery", nil, mock.AnythingOfType("gocbcore.SearchQueryOptions")).
		Run(runFn).
		Return(reader, nil)

	searchProvider := &searchProviderCore{
		provider: provider,
	}
	cli := new(mockConnectionManager)
	cli.On("getSearchProvider").Return(searchProvider, nil)

	cluster := suite.newCluster(cli)
	searchProvider.meter = cluster.meter
	searchProvider.tracer = cluster.tracer
	searchProvider.retryStrategyWrapper = cluster.retryStrategyWrapper
	searchProvider.timeouts = cluster.timeoutsConfig

	return cluster
}

func (suite *UnitTestSuite) TestSearchQuery() {
	var dataset testSearchDataset
	err := loadJSONTestDataset("beer_sample_search_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockSearchRowReader{
		Dataset: dataset.Hits,
		Meta:    suite.mustConvertToBytes(dataset.jsonSearchResponse),
		Suite:   suite,
	}

	query := search.NewTermQuery("term").Field("field").Fuzziness(1).Boost(2).PrefixLength(3)

	var cluster *Cluster
	cluster = suite.searchCluster(reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.SearchQueryOptions)
		suite.Assert().Equal(cluster.retryStrategyWrapper, opts.RetryStrategy)
		now := time.Now()
		if opts.Deadline.Before(now.Add(70*time.Second)) || opts.Deadline.After(now.Add(75*time.Second)) {
			suite.Fail("Deadline should have been <75s and >70s but was %s", opts.Deadline)
		}

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		if suite.Assert().Contains(actualOptions, "fields") {
			suite.Assert().Equal([]interface{}{"name"}, actualOptions["fields"])
		}

		if suite.Assert().Contains(actualOptions, "query") {
			q := actualOptions["query"].(map[string]interface{})
			suite.Assert().Equal("term", q["term"])
			suite.Assert().Equal("field", q["field"])
			suite.Assert().Equal(float64(1), q["fuzziness"])
			suite.Assert().Equal(float64(2), q["boost"])
			suite.Assert().Equal(float64(3), q["prefix_length"])
		}

		if suite.Assert().Contains(actualOptions, "sort") {
			s := actualOptions["sort"].([]interface{})
			suite.Require().Len(s, 1)
			srt := s[0].(map[string]interface{})
			suite.Assert().Equal("id", srt["by"])
			suite.Assert().Equal(true, srt["desc"])
		}
	})

	result, err := cluster.SearchQuery("testindex", query, &SearchOptions{
		Fields: []string{"name"},
		Facets: map[string]search.Facet{
			"type": search.NewTermFacet("country", 5),
		},
		Sort: []search.Sort{search.NewSearchSortID().Descending(true)},
	})
	suite.Require().Nil(err, err)
	suite.Require().NotNil(result)

	var hits []SearchRow
	for result.Next() {
		hit := result.Row()
		hits = append(hits, hit)
		var field struct {
			Name string
		}
		err := hit.Fields(&field)
		suite.Require().Nil(err, err)
		suite.Assert().NotEmpty(field.Name)
	}

	err = result.Err()
	suite.Require().Nil(err, err)

	suite.Assert().Len(hits, len(dataset.Hits))

	metadata, err := result.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Nil(metadata.Errors)
	suite.Assert().Equal(uint64(809), metadata.Metrics.TotalRows)
	suite.Assert().Equal(1.156383395549805, metadata.Metrics.MaxScore)
	suite.Assert().Equal(time.Duration(62511375), metadata.Metrics.Took)

	facets, err := result.Facets()
	suite.Require().Nil(err, err)

	expectedFacets := make(map[string]SearchFacetResult)
	for facetName, facetData := range dataset.Facets {
		var facet SearchFacetResult
		err := facet.fromData(facetData)
		suite.Require().Nil(err, err)

		expectedFacets[facetName] = facet
	}

	suite.Assert().Equal(expectedFacets, facets)
}

func (suite *UnitTestSuite) TestSearchQueryDisableScoring() {
	reader := &mockSearchRowReader{
		Dataset: []jsonSearchRow{},
		Meta:    []byte{},
		Suite:   suite,
	}

	query := search.NewMatchAllQuery()

	var cluster *Cluster
	cluster = suite.searchCluster(reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.SearchQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		if suite.Assert().Contains(actualOptions, "score") {
			suite.Assert().Equal("none", actualOptions["score"])
		}
	})

	_, err := cluster.SearchQuery("testindex", query, &SearchOptions{
		DisableScoring: true,
	})
	suite.Require().Nil(err, err)
}

func (suite *UnitTestSuite) TestSearchQueryNoScoringSet() {
	reader := &mockSearchRowReader{
		Dataset: []jsonSearchRow{},
		Meta:    []byte{},
		Suite:   suite,
	}

	query := search.NewMatchAllQuery()

	var cluster *Cluster
	cluster = suite.searchCluster(reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.SearchQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().NotContains(actualOptions, "score")
	})

	_, err := cluster.SearchQuery("testindex", query, &SearchOptions{})
	suite.Require().Nil(err, err)
}

func (suite *UnitTestSuite) TestSearchQueryExplicitlyEnableScoring() {
	reader := &mockSearchRowReader{
		Dataset: []jsonSearchRow{},
		Meta:    []byte{},
		Suite:   suite,
	}

	query := search.NewMatchAllQuery()

	var cluster *Cluster
	cluster = suite.searchCluster(reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.SearchQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Assert().NotContains(actualOptions, "score")
	})

	_, err := cluster.SearchQuery("testindex", query, &SearchOptions{
		DisableScoring: false,
	})
	suite.Require().Nil(err, err)
}

func (suite *UnitTestSuite) TestSearchQueryRaw() {
	var dataset testSearchDataset
	err := loadJSONTestDataset("beer_sample_search_dataset", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockSearchRowReader{
		Dataset: dataset.Hits,
		Meta:    suite.mustConvertToBytes(dataset.jsonSearchResponse),
		Suite:   suite,
	}

	query := search.NewTermQuery("term").Field("field").Fuzziness(1).Boost(2).PrefixLength(3)

	var cluster *Cluster
	cluster = suite.searchCluster(reader, func(args mock.Arguments) {})

	result, err := cluster.SearchQuery("testindex", query, &SearchOptions{
		Fields: []string{"name"},
		Facets: map[string]search.Facet{
			"type": search.NewTermFacet("country", 5),
		},
		Sort: []search.Sort{search.NewSearchSortID().Descending(true)},
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
		suite.Assert().Equal(suite.mustConvertToBytes(dataset.Hits[i]), b)
		i++
	}

	err = raw.Err()
	suite.Require().Nil(err, err)

	metadata, err := raw.MetaData()
	suite.Require().Nil(err, err)

	suite.Assert().Equal(reader.Meta, metadata)
}

func (suite *UnitTestSuite) TestSearchQueryCollections() {
	reader := &mockSearchRowReader{
		Dataset: []jsonSearchRow{},
		Meta:    []byte{},
		Suite:   suite,
	}

	query := search.NewMatchAllQuery()

	var cluster *Cluster
	cluster = suite.searchCluster(reader, func(args mock.Arguments) {
		opts := args.Get(1).(gocbcore.SearchQueryOptions)

		var actualOptions map[string]interface{}
		err := json.Unmarshal(opts.Payload, &actualOptions)
		suite.Require().Nil(err)

		suite.Require().Contains(actualOptions, "collections")
		collections := actualOptions["collections"].([]interface{})
		suite.Require().Len(collections, 2)
		suite.Assert().Equal("collection1", collections[0])
		suite.Assert().Equal("collection2", collections[1])
	})

	_, err := cluster.SearchQuery("testindex", query, &SearchOptions{
		Collections: []string{"collection1", "collection2"},
	})
	suite.Require().Nil(err, err)
}
