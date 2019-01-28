package gocb

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/couchbase/gocbcore.v8"
)

func TestBasicAnalyticsQuery(t *testing.T) {
	dataBytes, err := loadRawTestDataset("beer_sample_analytics_dataset")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var expectedResult analyticsResponse
	err = json.Unmarshal(dataBytes, &expectedResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset %v", err)
	}

	queryOptions := &AnalyticsQueryOptions{
		PositionalParameters: []interface{}{"brewery"},
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertAnalyticsQueryRequest(t, req)

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8095",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, 0, timeout, 0)

	res, err := cluster.AnalyticsQuery(statement, queryOptions)
	if err != nil {
		t.Fatal(err)
	}

	testAssertAnalyticsQueryResult(t, &expectedResult, res, true)
}

func TestAnalyticsQueryServiceNotFound(t *testing.T) {
	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		return nil, gocbcore.ErrNoCbasService
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	res, err := cluster.AnalyticsQuery(statement, nil)
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

func testAssertAnalyticsQueryRequest(t *testing.T, req *gocbcore.HttpRequest) {
	if req.Service != gocbcore.CbasService {
		t.Fatalf("Service should have been CbasService but was %d", req.Service)
	}

	if req.Context == nil {
		t.Fatalf("Context should not have been nil, but was")
	}

	_, ok := req.Context.Deadline()
	if !ok {
		t.Fatalf("Context should have had a deadline") // Difficult to test the actual deadline value
	}

	if req.Method != "POST" {
		t.Fatalf("Request method should have been POST but was %s", req.Method)
	}

	if req.Path != "/analytics/service" {
		t.Fatalf("Request path should have been /analytics/service but was %s", req.Path)
	}

	if req.Body == nil {
		t.Fatalf("Expected body to be non-nil")
	}
}

func testAssertAnalyticsQueryResult(t *testing.T, expectedResult *analyticsResponse, actualResult *AnalyticsResults, expectData bool) {
	if expectData {
		var breweryDocs []testBreweryDocument
		var resDoc testBreweryDocument
		for actualResult.Next(&resDoc) {
			breweryDocs = append(breweryDocs, resDoc)
		}

		var expectedDocs []testBreweryDocument
		for _, doc := range expectedResult.Results {
			var expectedDoc testBreweryDocument
			err := json.Unmarshal(doc, &expectedDoc)
			if err != nil {
				t.Fatalf("Unmarshalling expected result document failed %v", err)
			}
			expectedDocs = append(expectedDocs, expectedDoc)
		}

		if len(breweryDocs) != len(expectedResult.Results) {
			t.Fatalf("Expected results length to be %d but was %d", len(expectedResult.Results), len(breweryDocs))
		}

		for i, doc := range expectedDocs {
			if breweryDocs[i] != doc {
				t.Fatalf("Docs did not match, expected %v but was %v", doc, breweryDocs[i])
			}
		}
	}

	if actualResult.ClientContextID() != expectedResult.ClientContextID {
		t.Fatalf("Expected ClientContextID to be %s but was %s", expectedResult.ClientContextID, actualResult.ClientContextID())
	}

	if actualResult.RequestID() != expectedResult.RequestID {
		t.Fatalf("Expected RequestID to be %s but was %s", expectedResult.RequestID, actualResult.RequestID())
	}

	if actualResult.Status() != expectedResult.Status {
		t.Fatalf("Expected Status to be %s but was %s", expectedResult.Status, actualResult.Status())
	}

	metrics := actualResult.Metrics()
	elapsedTime, err := time.ParseDuration(expectedResult.Metrics.ElapsedTime)
	if err != nil {
		t.Fatalf("Failed to parse ElapsedTime %v", err)
	}
	if metrics.ElapsedTime != elapsedTime {
		t.Fatalf("Expected metrics ElapsedTime to be %s but was %s", metrics.ElapsedTime, elapsedTime)
	}

	executionTime, err := time.ParseDuration(expectedResult.Metrics.ExecutionTime)
	if err != nil {
		t.Fatalf("Failed to parse ElapsedTime %v", err)
	}
	if metrics.ExecutionTime != executionTime {
		t.Fatalf("Expected metrics ElapsedTime to be %s but was %s", metrics.ExecutionTime, executionTime)
	}

	if metrics.MutationCount != expectedResult.Metrics.MutationCount {
		t.Fatalf("Expected metrics MutationCount to be %d but was %d", metrics.MutationCount, expectedResult.Metrics.MutationCount)
	}

	if metrics.ErrorCount != expectedResult.Metrics.ErrorCount {
		t.Fatalf("Expected metrics ErrorCount to be %d but was %d", metrics.ErrorCount, expectedResult.Metrics.ErrorCount)
	}

	if metrics.ResultCount != expectedResult.Metrics.ResultCount {
		t.Fatalf("Expected metrics ResultCount to be %d but was %d", metrics.ResultCount, expectedResult.Metrics.ResultCount)
	}

	if metrics.ResultSize != expectedResult.Metrics.ResultSize {
		t.Fatalf("Expected metrics ResultSize to be %d but was %d", metrics.ResultSize, expectedResult.Metrics.ResultSize)
	}

	if metrics.SortCount != expectedResult.Metrics.SortCount {
		t.Fatalf("Expected metrics SortCount to be %d but was %d", metrics.SortCount, expectedResult.Metrics.SortCount)
	}

	if metrics.WarningCount != expectedResult.Metrics.WarningCount {
		t.Fatalf("Expected metrics WarningCount to be %d but was %d", metrics.WarningCount, expectedResult.Metrics.WarningCount)
	}

	if metrics.ProcessedObjects != expectedResult.Metrics.ProcessedObjects {
		t.Fatalf("Expected metrics ProcessedObjects to be %d but was %d", metrics.ProcessedObjects, expectedResult.Metrics.ProcessedObjects)
	}
}
