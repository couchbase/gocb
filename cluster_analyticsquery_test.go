package gocb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

// If the travel-sample dataset is not created up front then this can be a very slow test
// as it will create said dataset and then wait for it to become available.
func TestAnalyticsQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	if globalCluster.NotSupportsFeature(AnalyticsFeature) {
		t.Skip("Skipping test as analytics not supported.")
	}

	if globalTravelBucket == nil {
		t.Skip("Skipping test as no travel-sample bucket")
	}

	testCreateAnalyticsDataset(t)
	errCh := make(chan error)
	timer := time.NewTimer(30 * time.Second)
	go testWaitAnalyticsDataset(errCh)

	select {
	case <-timer.C:
		t.Fatalf("Wait time for analytics dataset to become ready expired")
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Failed to wait for analytics dataset to become ready: %v", err)
		}
	}

	t.Run("testSimpleAnalyticsQuery", testSimpleAnalyticsQuery)
	t.Run("testSimpleAnalyticsQueryOne", testSimpleAnalyticsQueryOne)
	t.Run("testSimpleAnalyticsQueryNone", testSimpleAnalyticsQueryNone)
	t.Run("testSimpleAnalyticsQueryOneNone", testSimpleAnalyticsQueryOneNone)
	t.Run("testSimpleAnalyticsQueryError", testSimpleAnalyticsQueryError)
	t.Run("testAnalyticsQueryNamedParameters", testAnalyticsQueryNamedParameters)
	t.Run("testAnalyticsQueryPositionalParameters", testAnalyticsQueryPositionalParameters)
}

func testCreateAnalyticsDataset(t *testing.T) {
	// _p/cbas-admin/analytics/node/agg/stats/remaining
	query := "CREATE DATASET `travel-sample` ON `travel-sample`;"
	res, err := globalCluster.AnalyticsQuery(query, nil)
	if err != nil {
		aErr, ok := err.(AnalyticsQueryError)
		if !ok {
			t.Fatalf("Failed to create dataset: %v", err)
		}

		// 24040 means that this dataset already exists, which is a-ok
		if aErr.Code() != 24040 {
			t.Fatalf("Failed to create dataset: %v", err)
		}
	}

	if res != nil {
		err = res.Close()
		if err != nil {
			t.Fatalf("Failed to close result: %v", err)
		}
	}

	query = "CONNECT LINK Local;"
	_, err = globalCluster.AnalyticsQuery(query, nil)
	if err != nil {
		t.Fatalf("Failed to connect link %v", err)
	}
}

func testWaitAnalyticsDataset(errCh chan error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.MgmtService,
		Path:    "/_p/cbas-admin/analytics/node/agg/stats/remaining",
		Method:  "GET",
	}

	agent, err := globalCluster.getHTTPProvider()
	if err != nil {
		errCh <- fmt.Errorf("failed to get HTTP provider: %v", err)
	}

	for {
		time.Sleep(250 * time.Millisecond)

		res, err := agent.DoHttpRequest(req)
		if err != nil {
			errCh <- fmt.Errorf("failed to execute HTTP request: %v", err)
		}

		decoder := json.NewDecoder(res.Body)
		var indexRemaining map[string]int
		err = decoder.Decode(&indexRemaining)
		if err != nil {
			errCh <- fmt.Errorf("failed to decode response: %v", err)
		}

		remaining, ok := indexRemaining["Default.travel-sample"]
		if !ok {
			errCh <- fmt.Errorf("missing Default.travel-sample entry from index remaining")
		}

		if remaining == 0 {
			break
		}
	}

	errCh <- nil
}

// In these tests use a large enough limit to force streaming to occur.
func testSimpleAnalyticsQuery(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 10000;"
	result, err := globalCluster.AnalyticsQuery(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for result.Next(&sample) {
		samples = append(samples, sample)
	}

	err = result.Close()
	if err != nil {
		t.Fatalf("Rows close had error: %v", err)
	}

	if len(samples) != 10000 {
		t.Fatalf("Expected result to contain 10000 documents but had %d", len(samples))
	}

	metadata, err := result.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleAnalyticsQueryOne(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 10000;"
	rows, err := globalCluster.AnalyticsQuery(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var sample interface{}
	err = rows.One(&sample)
	if err != nil {
		t.Fatalf("Reading row had error: %v", err)
	}

	if sample == nil {
		t.Fatalf("Expected sample to be not nil")
	}

	metadata, err := rows.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleAnalyticsQueryOneNone(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` WHERE `name` = \"Idontexist\" LIMIT 10000;"
	rows, err := globalCluster.AnalyticsQuery(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var sample interface{}
	err = rows.One(&sample)
	if err == nil {
		t.Fatalf("Expected One to return error")
	}

	if !IsNoResultsError(err) {
		t.Fatalf("Expected error to be no results but was %v", err)
	}

	if sample != nil {
		t.Fatalf("Expected sample to be nil but was %v", sample)
	}

	metadata, err := rows.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleAnalyticsQueryNone(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` WHERE `name` = \"Idontexist\" LIMIT 10000;"
	rows, err := globalCluster.AnalyticsQuery(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for rows.Next(&sample) {
		samples = append(samples, sample)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Rows close had error: %v", err)
	}

	if len(samples) != 0 {
		t.Fatalf("Expected result to contain 0 documents but had %d", len(samples))
	}

	metadata, err := rows.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleAnalyticsQueryError(t *testing.T) {
	query := "SELECT `travel-sample`. FROM `travel-sample` LIMIT 10000;"
	_, err := globalCluster.AnalyticsQuery(query, nil)
	if err == nil {
		t.Fatalf("Expected execute query to error")
	}
}

func testAnalyticsQueryNamedParameters(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` where `type`=$t AND `name`=$name LIMIT 10000;"
	params := make(map[string]interface{}, 1)
	params["t"] = "hotel"
	params["$name"] = "Medway Youth Hostel"
	rows, err := globalCluster.AnalyticsQuery(query, &AnalyticsQueryOptions{NamedParameters: params})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for rows.Next(&sample) {
		samples = append(samples, sample)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Rows close had error: %v", err)
	}

	if len(samples) != 1 {
		t.Fatalf("Expected breweries to contain 1 document but had %d", len(samples))
	}

	metadata, err := rows.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testAnalyticsQueryPositionalParameters(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` where `type`=? AND `name`=? LIMIT 10000;"
	rows, err := globalCluster.AnalyticsQuery(query, &AnalyticsQueryOptions{PositionalParameters: []interface{}{"hotel", "Medway Youth Hostel"}})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for rows.Next(&sample) {
		samples = append(samples, sample)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Rows close had error: %v", err)
	}

	if len(samples) != 1 {
		t.Fatalf("Expected breweries to contain 1 document but had %d", len(samples))
	}

	metadata, err := rows.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

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

func TestBasicAnalyticsQuerySerializer(t *testing.T) {
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
		Serializer:           &DefaultJSONSerializer{},
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
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

	if !IsServiceNotAvailableError(err) {
		t.Fatalf("Expected error to be ServiceNotFoundError but was %v", err)
	}
}

func TestAnalyticsQueryClientSideTimeout(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 20 * time.Millisecond
	clusterTimeout := 50 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertAnalyticsQueryRequest(t, req)

		var opts map[string]interface{}
		err := json.Unmarshal(req.Body, &opts)
		if err != nil {
			t.Fatalf("Failed to unmarshal request body %v", err)
		}

		optsTimeout, ok := opts["timeout"]
		if !ok {
			t.Fatalf("Request query options missing timeout")
		}

		dur, err := time.ParseDuration(optsTimeout.(string))
		if err != nil {
			t.Fatalf("Could not parse timeout: %v", err)
		}

		if dur < (timeout-50*time.Millisecond) || dur > (timeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", timeout.String(), optsTimeout)
		}

		// we can't use time travel here as we need the context to actually timeout
		time.Sleep(100 * time.Millisecond)

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err := cluster.AnalyticsQuery(statement, &AnalyticsQueryOptions{
		ServerSideTimeout: timeout,
		Context:           ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestAnalyticsQueryStreamTimeout(t *testing.T) {
	dataBytes, err := loadRawTestDataset("analytics_timeout")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 20 * time.Millisecond
	clusterTimeout := 50 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertAnalyticsQueryRequest(t, req)

		var opts map[string]interface{}
		err := json.Unmarshal(req.Body, &opts)
		if err != nil {
			t.Fatalf("Failed to unmarshal request body %v", err)
		}

		optsTimeout, ok := opts["timeout"]
		if !ok {
			t.Fatalf("Request query options missing timeout")
		}

		dur, err := time.ParseDuration(optsTimeout.(string))
		if err != nil {
			t.Fatalf("Could not parse timeout: %v", err)
		}

		if dur < (timeout-50*time.Millisecond) || dur > (timeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", timeout.String(), optsTimeout)
		}

		resp := &gocbcore.HttpResponse{
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}

		return resp, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err = cluster.AnalyticsQuery(statement, &AnalyticsQueryOptions{
		ServerSideTimeout: timeout,
		Context:           ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatalf("Error should have been timeout but was %v", err)
	}
}

func TestAnalyticsQueryConnectContextTimeout(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 50 * time.Second
	clusterTimeout := 50 * time.Second
	ctxTimeout := 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertAnalyticsQueryRequest(t, req)

		var opts map[string]interface{}
		err := json.Unmarshal(req.Body, &opts)
		if err != nil {
			t.Fatalf("Failed to unmarshal request body %v", err)
		}

		optsTimeout, ok := opts["timeout"]
		if !ok {
			t.Fatalf("Request query options missing timeout")
		}

		dur, err := time.ParseDuration(optsTimeout.(string))
		if err != nil {
			t.Fatalf("Could not parse timeout: %v", err)
		}

		if dur < (ctxTimeout-50*time.Millisecond) || dur > (ctxTimeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", ctxTimeout.String(), optsTimeout)
		}

		// we can't use time travel here as we need the context to actually timeout
		time.Sleep(100 * time.Millisecond)

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err := cluster.AnalyticsQuery(statement, &AnalyticsQueryOptions{
		ServerSideTimeout: timeout,
		Context:           ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestAnalyticsQueryConnectClusterTimeoutClusterWins(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	clusterTimeout := 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertAnalyticsQueryRequest(t, req)

		var opts map[string]interface{}
		err := json.Unmarshal(req.Body, &opts)
		if err != nil {
			t.Fatalf("Failed to unmarshal request body %v", err)
		}

		optsTimeout, ok := opts["timeout"]
		if !ok {
			t.Fatalf("Request query options missing timeout")
		}

		dur, err := time.ParseDuration(optsTimeout.(string))
		if err != nil {
			t.Fatalf("Could not parse timeout: %v", err)
		}

		if dur < (clusterTimeout-50*time.Millisecond) || dur > (clusterTimeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", clusterTimeout.String(), optsTimeout)
		}

		// we can't use time travel here as we need the context to actually timeout
		time.Sleep(100 * time.Millisecond)

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err := cluster.AnalyticsQuery(statement, &AnalyticsQueryOptions{
		Context: ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestAnalyticsQueryConnectClusterTimeoutContextWins(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	clusterTimeout := 40 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertAnalyticsQueryRequest(t, req)

		var opts map[string]interface{}
		err := json.Unmarshal(req.Body, &opts)
		if err != nil {
			t.Fatalf("Failed to unmarshal request body %v", err)
		}

		optsTimeout, ok := opts["timeout"]
		if !ok {
			t.Fatalf("Request query options missing timeout")
		}

		dur, err := time.ParseDuration(optsTimeout.(string))
		if err != nil {
			t.Fatalf("Could not parse timeout: %v", err)
		}

		if dur < (clusterTimeout-50*time.Millisecond) || dur > (clusterTimeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", clusterTimeout.String(), optsTimeout)
		}

		// we can't use time travel here as we need the context to actually timeout
		time.Sleep(100 * time.Millisecond)

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err := cluster.AnalyticsQuery(statement, &AnalyticsQueryOptions{
		Context: ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func testAssertAnalyticsQueryRequest(t *testing.T, req *gocbcore.HttpRequest) {
	if req.Service != gocbcore.CbasService {
		t.Fatalf("Service should have been AnalyticsService but was %d", req.Service)
	}

	if req.Context == nil {
		t.Fatalf("Context should not have been nil, but was")
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
	var breweryDocs []testBreweryDocument
	var resDoc testBreweryDocument
	for actualResult.Next(&resDoc) {
		breweryDocs = append(breweryDocs, resDoc)
	}

	err := actualResult.Close()
	if err != nil {
		t.Fatalf("expected err to be nil but was %v", err)
	}

	if expectData {
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

	metadata, err := actualResult.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.ClientContextID() != expectedResult.ClientContextID {
		t.Fatalf("Expected ClientContextID to be %s but was %s", expectedResult.ClientContextID, metadata.ClientContextID())
	}

	if metadata.RequestID() != expectedResult.RequestID {
		t.Fatalf("Expected RequestID to be %s but was %s", expectedResult.RequestID, metadata.RequestID())
	}

	if metadata.Status() != expectedResult.Status {
		t.Fatalf("Expected Status to be %s but was %s", expectedResult.Status, metadata.Status())
	}

	metrics := metadata.Metrics()
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
