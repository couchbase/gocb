package gocb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/couchbase/gocbcore.v8"
)

func TestQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	if globalCluster.NotSupportsFeature(N1qlFeature) {
		t.Skip("Skipping test as n1ql not supported.")
	}

	if globalTravelBucket == nil {
		t.Skip("Skipping test as no travel-sample bucket")
	}

	t.Run("testSimpleQuery", testSimpleQuery)
	t.Run("testPreparedQuery", testPreparedQuery)
	t.Run("testQueryNamedParameters", testQueryNamedParameters)
	t.Run("testQueryPositionalParameters", testQueryPositionalParameters)
}

func testSimpleQuery(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 10;"
	rows, err := globalCluster.Query(query, nil)
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

	if len(samples) != 10 {
		t.Fatalf("Expected result to contain 10 documents but had %d", len(samples))
	}

	if rows.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}

	if rows.SourceEndpoint() == "" {
		t.Fatalf("Result should have had non empty SourceEndpoint")
	}
}

func testPreparedQuery(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 10;"
	rows, err := globalCluster.Query(query, &QueryOptions{Prepared: true})
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

	if len(samples) != 10 {
		t.Fatalf("Expected result to contain 10 documents but had %d", len(samples))
	}

	if rows.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}

	if rows.SourceEndpoint() == "" {
		t.Fatalf("Result should have had non empty SourceEndpoint")
	}

	if globalCluster.queryCache[query] == nil {
		t.Fatalf("Query should have been in query cache after prepared statement execution")
	}
}

func testQueryNamedParameters(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` where `type`=$type AND `name`=$name LIMIT 10;"
	params := make(map[string]interface{}, 1)
	params["type"] = "hotel"
	params["name"] = "Medway Youth Hostel"
	rows, err := globalCluster.Query(query, &QueryOptions{NamedParameters: params})
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

	if rows.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}

	if rows.SourceEndpoint() == "" {
		t.Fatalf("Result should have had non empty SourceEndpoint")
	}
}

func testQueryPositionalParameters(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` where `type`=? AND `name`=? LIMIT 10;"
	rows, err := globalCluster.Query(query, &QueryOptions{PositionalParameters: []interface{}{"hotel", "Medway Youth Hostel"}})
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

	if rows.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}

	if rows.SourceEndpoint() == "" {
		t.Fatalf("Result should have had non empty SourceEndpoint")
	}
}

func TestBasicQuery(t *testing.T) {
	dataBytes, err := loadRawTestDataset("beer_sample_query_dataset")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var expectedResult n1qlResponse
	err = json.Unmarshal(dataBytes, &expectedResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset %v", err)
	}

	queryOptions := &QueryOptions{
		PositionalParameters: []interface{}{"brewery"},
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)

		var opts map[string]interface{}
		err := json.Unmarshal(req.Body, &opts)
		if err != nil {
			t.Fatalf("Failed to unmarshal request body %v", err)
		}

		if len(opts) != 3 {
			t.Fatalf("Expected request body to contain 3 options but was %d, %v", len(opts), opts)
		}

		optsStatement, ok := opts["statement"]
		if !ok {
			t.Fatalf("Request query options missing statement")
		}
		if optsStatement != statement {
			t.Fatalf("Expected statement to be %s but was %s", statement, optsStatement)
		}
		optsTimeout, ok := opts["timeout"]
		if !ok {
			t.Fatalf("Request query options missing timeout")
		}
		optsDuration, err := time.ParseDuration(optsTimeout.(string))
		if err != nil {
			t.Fatalf("Failed to parse request timeout %v", err)
		}

		if optsDuration < (timeout-50*time.Millisecond) || optsDuration > (timeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", timeout, optsDuration)
		}

		optsParams, ok := opts["args"].([]interface{})
		if !ok {
			t.Fatalf("Request query options missing args")
		}
		if len(optsParams) != 1 {
			t.Fatalf("Expected args to be length 1 but was %d", len(optsParams))
		}
		if optsParams[0] != "brewery" {
			t.Fatalf("Expected args content to be brewery but was %s", optsParams[0])
		}

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

	res, err := cluster.Query(statement, queryOptions)
	if err != nil {
		t.Fatal(err)
	}

	testAssertQueryResult(t, &expectedResult, res, true)
}

func TestQueryError(t *testing.T) {
	dataBytes, err := loadRawTestDataset("beer_sample_query_error")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var expectedResult n1qlResponse
	err = json.Unmarshal(dataBytes, &expectedResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset %v", err)
	}

	queryOptions := &QueryOptions{
		PositionalParameters: []interface{}{"brewery"},
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 400,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	res, err := cluster.Query(statement, queryOptions)
	if err == nil {
		t.Fatal("Expected query to return error")
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
	}

	queryErrs, ok := err.(QueryErrors)
	if !ok {
		t.Fatalf("Expected error to be QueryErrors but was %s", reflect.TypeOf(err).String())
	}

	if queryErrs.Endpoint() != "localhost:8093" {
		t.Fatalf("Expected error endpoint to be localhost:8093 but was %s", queryErrs.Endpoint())
	}

	if queryErrs.HTTPStatus() != 400 {
		t.Fatalf("Expected error HTTP status to be 400 but was %d", queryErrs.HTTPStatus())
	}

	if queryErrs.ContextID() != expectedResult.ClientContextID {
		t.Fatalf("Expected error ContextID to be %s but was %s", expectedResult.ClientContextID, queryErrs.ContextID())
	}

	if len(queryErrs.Errors()) != len(expectedResult.Errors) {
		t.Fatalf("Expected errors to contain 1 error but contained %d", len(queryErrs.Errors()))
	}

	var errs []string
	errors := queryErrs.Errors()
	for i, err := range expectedResult.Errors {
		msg := fmt.Sprintf("[%d] %s", err.ErrorCode, err.ErrorMessage)
		errs = append(errs, msg)

		if errors[i].Code() != err.ErrorCode {
			t.Fatalf("Expected error code to be %d but was %d", errors[i].Code(), err.ErrorCode)
		}

		if errors[i].Message() != err.ErrorMessage {
			t.Fatalf("Expected error message to be %s but was %s", errors[i].Message(), err.ErrorMessage)
		}

		if errors[i].Error() != msg {
			t.Fatalf("Expected error Error() to be %s but was %s", errors[i].Error(), msg)
		}
	}
	joinedErrs := strings.Join(errs, ", ")
	if queryErrs.Error() != joinedErrs {
		t.Fatalf("Expected error Error() to be %s but was %s", joinedErrs, queryErrs.Error())
	}
}

func TestQueryServiceNotFound(t *testing.T) {
	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		return nil, gocbcore.ErrNoN1qlService
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	res, err := cluster.Query(statement, nil)
	if err == nil {
		t.Fatal("Expected query to return error")
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
	}

	if !IsServiceNotFoundError(err) {
		t.Fatalf("Expected error to be ServiceNotFoundError but was %s", reflect.TypeOf(err).Name())
	}
}

func TestQueryTimeout(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 40 * time.Second
	clusterTimeout := 50 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)

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

		cancel()
		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err := cluster.Query(statement, &QueryOptions{
		Timeout: timeout,
		Context: ctx,
	})
	if err != nil && !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestQueryContextTimeout(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 50 * time.Second
	clusterTimeout := 50 * time.Second
	ctxTimeout := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)

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

		cancel()
		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err := cluster.Query(statement, &QueryOptions{
		Timeout: timeout,
		Context: ctx,
	})
	if err != nil && !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestQueryClusterTimeout(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 50 * time.Second
	clusterTimeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)

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

		cancel()
		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err := cluster.Query(statement, &QueryOptions{
		Timeout: timeout,
		Context: ctx,
	})
	if err != nil && !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func testAssertQueryRequest(t *testing.T, req *gocbcore.HttpRequest) {
	if req.Service != gocbcore.N1qlService {
		t.Fatalf("Service should have been N1qlService but was %d", req.Service)
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

	if req.Path != "/query/service" {
		t.Fatalf("Request path should have been /query/service but was %s", req.Path)
	}
}

func testAssertQueryResult(t *testing.T, expectedResult *n1qlResponse, actualResult *QueryResults, expectData bool) {
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
	if actualResult.SourceEndpoint() != "localhost:8093" {
		t.Fatalf("Expected endpoint to be %s but was %s", "localhost:8093", actualResult.SourceEndpoint())
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
}

func testGetClusterForHTTP(provider *mockHTTPProvider, n1qlTimeout, analyticsTimeout, searchTimeout time.Duration) *Cluster {
	clients := make(map[string]client)
	cli := &mockClient{
		bucketName:        "mock",
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: false,
		mockHTTPProvider:  provider,
	}
	clients["mock-false"] = cli
	c := &Cluster{
		connections: clients,
	}
	c.sb.QueryTimeout = n1qlTimeout
	c.sb.AnalyticsTimeout = analyticsTimeout
	c.sb.SearchTimeout = searchTimeout

	return c
}
