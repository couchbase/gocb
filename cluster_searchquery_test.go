package gocb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

func TestSearchQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	if globalCluster.NotSupportsFeature(FtsFeature) {
		t.Skip("Skipping test as analytics not supported.")
	}

	if globalTravelBucket == nil {
		t.Skip("Skipping test as no travel-sample bucket")
	}

	testCreateSearchIndexes(t)
	errCh := make(chan error)
	timer := time.NewTimer(30 * time.Second)
	go testWaitSearchIndex(errCh)

	select {
	case <-timer.C:
		t.Fatalf("Wait time for analytics dataset to become ready expired")
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Failed to wait for analytics dataset to become ready: %v", err)
		}
	}

	t.Run("testSimpleSearchQuery", testSimpleSearchQuery)
	t.Run("testSimpleSearchQueryOne", testSimpleSearchQueryOne)
	t.Run("testSimpleSearchQueryError", testSimpleSearchQueryError)
}

func testCreateSearchIndexes(t *testing.T) {
	testCreateSearchIndex(t, "travel-sample-index-unstored")
}

func testCreateSearchIndex(t *testing.T, indexName string) {
	index, err := loadRawTestDataset(indexName)
	if err != nil {
		t.Fatalf("failed to load index %v", err)
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.FtsService,
		Path:    "/api/index/" + indexName,
		Method:  "PUT",
		Body:    index,
	}

	agent, err := globalCluster.getHTTPProvider()
	if err != nil {
		t.Fatalf("failed to get HTTP provider: %v", err)
	}

	_, err = agent.DoHttpRequest(req)
	if err != nil {
		t.Fatalf("failed to execute HTTP request: %v", err)
	}
}

func testWaitSearchIndex(errCh chan error) {
	indexName := "travel-sample-index-unstored"
	req := &gocbcore.HttpRequest{
		Service: gocbcore.MgmtService,
		Path:    fmt.Sprintf("/_p/fts/api/index/%s/count", indexName),
		Method:  "GET",
	}

	agent, err := globalCluster.getHTTPProvider()
	if err != nil {
		errCh <- fmt.Errorf("failed to get HTTP provider: %v", err)
	}

	type count struct {
		Status string `json:"status"`
		Count  int    `json:"count"`
	}

	for {
		time.Sleep(250 * time.Millisecond)

		res, err := agent.DoHttpRequest(req)
		if err != nil {
			errCh <- fmt.Errorf("failed to execute HTTP request: %v", err)
		}

		decoder := json.NewDecoder(res.Body)

		var indexCount count
		err = decoder.Decode(&indexCount)
		if err != nil {
			errCh <- fmt.Errorf("failed to decode response: %v", err)
		}

		// we don't need to wait until everything is indexed
		if indexCount.Count > 1000 {
			break
		}
	}

	errCh <- nil
}

func testSimpleSearchQuery(t *testing.T) {
	indexName := "travel-sample-index-unstored"
	query := SearchQuery{Name: indexName, Query: NewMatchQuery("swanky")}

	results, err := globalCluster.SearchQuery(query, &SearchQueryOptions{Limit: 1000})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []SearchResultHit
	var sample SearchResultHit
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err != nil {
		t.Fatalf("results close had error: %v", err)
	}

	if len(samples) == 0 {
		t.Fatalf("Expected result to contain documents but had none")
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.TotalHits() == 0 {
		t.Fatalf("Expected result TotalRows to be not 0 but was")
	}
}

func testSimpleSearchQueryOne(t *testing.T) {
	indexName := "travel-sample-index-unstored"
	query := SearchQuery{Name: indexName, Query: NewMatchQuery("swanky")}

	results, err := globalCluster.SearchQuery(query, &SearchQueryOptions{Limit: 1000})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	sample := SearchResultHit{}
	err = results.One(&sample)
	if err != nil {
		t.Fatalf("One had error: %v", err)
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.TotalHits() == 0 {
		t.Fatalf("Expected result TotalRows to be not 0 but was")
	}
}

func testSimpleSearchQueryError(t *testing.T) {
	indexName := "travel-sample-index-unsored"
	query := SearchQuery{Name: indexName, Query: NewMatchQuery("swanky")}

	results, err := globalCluster.SearchQuery(query, &SearchQueryOptions{Limit: 1000})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []SearchResultHit
	var sample SearchResultHit
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err == nil {
		t.Fatalf("Expected results close should to have error")
	}

	_, ok := err.(SearchErrors)
	if !ok {
		t.Fatalf("Expected error to be SearchErrors but was %s", reflect.TypeOf(err).String())
	}

	if len(samples) != 0 {
		t.Fatalf("Expected result to contain 0 documents but had %d", len(samples))
	}
}

func TestSearchQueryServiceNotFound(t *testing.T) {
	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		return nil, gocbcore.ErrNoFtsService
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	q := SearchQuery{
		Name:  "test",
		Query: NewMatchQuery("test"),
	}
	timeout := 60 * time.Second

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	res, err := cluster.SearchQuery(q, nil)
	if err == nil {
		t.Fatal("Expected query to return error")
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
	}

	if !IsServiceNotFoundError(err) {
		t.Fatalf("Expected error to be ServiceNotFoundError but was %v", err)
	}
}
