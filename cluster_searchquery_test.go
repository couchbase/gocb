package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	testCreateSearchIndex(t, "travel-sample-index-stored")
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
	indexName := "travel-sample-index-stored"
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
	indexName := "travel-sample-index-stored"
	query := SearchQuery{Name: indexName, Query: NewMatchQuery("airline_137")}

	results, err := globalCluster.SearchQuery(query, &SearchQueryOptions{Limit: 1000, Fields: []string{"*"}})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []SearchResultHit
	var sample SearchResultHit
	for results.Next(&sample) {
		samples = append(samples, sample)

		if sample.Fields == nil {
			t.Fatalf("Expected fields to be not nil")
		}
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
	indexName := "travel-sample-index-stored"
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

	res, err := globalCluster.SearchQuery(query, &SearchQueryOptions{Limit: 1000})
	if err == nil {
		t.Fatalf("Execute query should have errored")
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
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

	if !IsServiceNotAvailableError(err) {
		t.Fatalf("Expected error to be ServiceNotFoundError but was %v", err)
	}
}

func TestSearchQueryRetries(t *testing.T) {
	q := SearchQuery{
		Name:  "test",
		Query: NewMatchQuery("test"),
	}
	timeout := 60 * time.Second

	var retries int

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		retries++

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 419,
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)
	cluster.sb.SearchRetryBehavior = StandardDelayRetryBehavior(3, 1, 100*time.Millisecond, LinearDelayFunction)

	_, err := cluster.SearchQuery(q, nil)
	if err == nil {
		t.Fatal("Expected query execution to error")
	}

	if retries != 3 {
		t.Fatalf("Expected query to be retried 3 time but ws retried %d times", retries)
	}
}

func TestSearchQueryServerObjectError(t *testing.T) {
	q := SearchQuery{
		Name:  "test",
		Query: NewMatchQuery("test"),
	}
	timeout := 60 * time.Second

	_, dataBytes, err := loadSDKTestDataset("search/alltimeouts")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	res, err := cluster.SearchQuery(q, nil)
	if err == nil {
		t.Fatal("Expected query execution to error")
	}

	if searchErr, ok := err.(SearchErrors); ok {
		if len(searchErr.Errors()) != 6 {
			t.Fatalf("Expected length of search errors to be 6 but was %d", len(searchErr.Errors()))
		}
	} else {
		t.Fatalf("Expected error to be SearchErrors but was %v", err)
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
	}
}

func TestSearchQuery400Error(t *testing.T) {
	q := SearchQuery{
		Name:  "test",
		Query: NewMatchQuery("test"),
	}
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 400,
			Body:       &testReadCloser{bytes.NewBuffer([]byte("an error")), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	res, err := cluster.SearchQuery(q, nil)
	if err == nil {
		t.Fatal("Expected query execution to error")
	}

	if searchErrs, ok := err.(SearchErrors); ok {
		if len(searchErrs.Errors()) != 1 {
			t.Fatalf("Expected length of search errors to be 6 but was %d", len(searchErrs.Errors()))
		}

		if searchErr, ok := searchErrs.Errors()[0].(SearchError); ok {
			if searchErr.Message() != "an error" {
				t.Fatalf("Expected error message to be \"an error\" but was %s", searchErr.Message())
			}
		} else {
			t.Fatalf("Expected search error to be SearchError but was %v", err)
		}
	} else {
		t.Fatalf("Expected search errors to be SearchErrors but was %v", err)
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
	}
}

func TestSearchQuery401Error(t *testing.T) {
	q := SearchQuery{
		Name:  "test",
		Query: NewMatchQuery("test"),
	}
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 401,
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	res, err := cluster.SearchQuery(q, nil)
	if err == nil {
		t.Fatal("Expected query execution to error")
	}

	if searchErrs, ok := err.(SearchErrors); ok {
		if len(searchErrs.Errors()) != 1 {
			t.Fatalf("Expected length of search errors to be 6 but was %d", len(searchErrs.Errors()))
		}

		if searchErr, ok := searchErrs.Errors()[0].(SearchError); ok {
			if searchErr.Message() != "The requested consistency level could not be "+
				"satisfied before the timeout was reached" {
				t.Fatalf(
					"Expected error message to be \"The requested consistency level could not be "+
						"satisfied before the timeout was reached\" but was %s",
					searchErr.Message(),
				)
			}
		} else {
			t.Fatalf("Expected search error to be SearchError but was %v", err)
		}
	} else {
		t.Fatalf("Expected search errors to be SearchErrors but was %v", err)
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
	}
}
