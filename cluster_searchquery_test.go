package gocb

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/stretchr/testify/mock"

	"github.com/couchbase/gocb/v2/search"
)

func (suite *IntegrationTestSuite) TestSearch() {
	suite.skipIfUnsupported(SearchFeature)

	n := suite.setupSearch()
	suite.runSearchTest(n)
}

func (suite *IntegrationTestSuite) runSearchTest(n int) {
	deadline := time.Now().Add(10 * time.Second)
	query := search.NewTermQuery("search").Field("service")
	var result *SearchResult
	for {
		var err error
		result, err = globalCluster.SearchQuery("search_test_index", query, &SearchOptions{
			Timeout: 1 * time.Second,
			Facets: map[string]search.Facet{
				"type": search.NewTermFacet("country", 5),
			},
		})
		if err != nil {
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

		var ids []string
		for result.Next() {
			row := result.Row()
			ids = append(ids, row.ID)
		}

		err = result.Err()
		suite.Require().Nil(err, err)

		if n == len(ids) {
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
		suite.Assert().NotEmpty(f.Total)
	}
}

func (suite *IntegrationTestSuite) setupSearch() int {
	n, err := suite.createBreweryDataset("beer_sample_brewery_five", "search")
	suite.Require().Nil(err, err)

	mgr := globalCluster.SearchIndexes()
	err = mgr.UpsertIndex(SearchIndex{
		Name:       "search_test_index",
		SourceName: globalBucket.Name(),
		SourceType: "couchbase",
		Type:       "fulltext-index",
	}, &UpsertSearchIndexOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	return n
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
	cluster := suite.newCluster()

	provider := new(mockSearchProvider)
	provider.
		On("SearchQuery", mock.AnythingOfType("gocbcore.SearchQueryOptions")).
		Run(runFn).
		Return(reader, nil)

	cli := new(mockClient)
	cli.On("getSearchProvider").Return(provider, nil)
	cli.On("supportsGCCCP").Return(true)

	cluster.clusterClient = cli

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
		opts := args.Get(0).(gocbcore.SearchQueryOptions)
		suite.Assert().Equal(cluster.sb.RetryStrategyWrapper, opts.RetryStrategy)
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

	var meta SearchMetaData
	err = meta.fromData(dataset.jsonSearchResponse)
	suite.Require().Nil(err, err)
	suite.Assert().Equal(&meta, metadata)

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
