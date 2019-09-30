package gocb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
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
	t.Run("testSimpleQueryTimeout", testSimpleQueryStreamTimeout)
	t.Run("testSimpleQueryContextTimeout", testSimpleQueryStreamContextTimeout)
	t.Run("testSimpleQueryOne", testSimpleQueryOne)
	t.Run("testSimpleQueryNone", testSimpleQueryNone)
	t.Run("testSimpleQueryOneNone", testSimpleQueryOneNone)
	t.Run("testSimpleQueryError", testSimpleQueryError)
	t.Run("testPreparedQuery", testPreparedQuery)
	t.Run("testQueryNamedParameters", testQueryNamedParameters)
	t.Run("testQueryPositionalParameters", testQueryPositionalParameters)
}

// In these tests use a large enough limit to force streaming to occur.
func testSimpleQuery(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 10000;"
	results, err := globalCluster.Query(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err != nil {
		t.Fatalf("results close had error: %v", err)
	}

	if len(samples) != 10000 {
		t.Fatalf("Expected result to contain 10000 documents but had %d", len(samples))
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleQueryStreamTimeout(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 30000;"
	results, err := globalCluster.Query(query, &QueryOptions{
		Timeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	globalCluster.TimeTravel(1 * time.Second)

	var samples []interface{}
	var sample interface{}
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err == nil {
		t.Fatalf("results close should have errored")
	}

	if !IsTimeoutError(err) {
		t.Fatalf("Expected error to be timeout but was %v", err)
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleQueryStreamContextTimeout(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 10000;"
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	results, err := globalCluster.Query(query, &QueryOptions{
		Context: ctx,
	})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	globalCluster.TimeTravel(1 * time.Second)

	var samples []interface{}
	var sample interface{}
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err == nil {
		t.Fatalf("results close should have errored")
	}

	if !IsTimeoutError(err) {
		t.Fatalf("Expected error to be timeout but was %v", err)
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleQueryOne(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 10000;"
	results, err := globalCluster.Query(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var sample interface{}
	err = results.One(&sample)
	if err != nil {
		t.Fatalf("Reading row had error: %v", err)
	}

	if sample == nil {
		t.Fatalf("Expected sample to be not nil")
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleQueryOneNone(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` WHERE `name` = \"Idontexist\" LIMIT 10000;"
	results, err := globalCluster.Query(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var sample interface{}
	err = results.One(&sample)
	if err == nil {
		t.Fatalf("Expected One to return error")
	}

	if !IsNoResultsError(err) {
		t.Fatalf("Expected error to be no results but was %v", err)
	}

	if sample != nil {
		t.Fatalf("Expected sample to be nil but was %v", sample)
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleQueryNone(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` WHERE `name` = \"Idontexist\" LIMIT 10000;"
	results, err := globalCluster.Query(query, nil)
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err != nil {
		t.Fatalf("results close had error: %v", err)
	}

	if len(samples) != 0 {
		t.Fatalf("Expected result to contain 0 documents but had %d", len(samples))
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testSimpleQueryError(t *testing.T) {
	query := "SELECT `travel-sample`. FROM `travel-sample` LIMIT 10000;"
	_, err := globalCluster.Query(query, nil)
	if err == nil {
		t.Fatalf("Expected execute query to error")
	}
}

func testPreparedQuery(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` LIMIT 10000;"
	results, err := globalCluster.Query(query, &QueryOptions{Prepared: true})
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err != nil {
		t.Fatalf("results close had error: %v", err)
	}

	if len(samples) != 10000 {
		t.Fatalf("Expected result to contain 10000 documents but had %d", len(samples))
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}

	if globalCluster.queryCache[query] == nil {
		t.Fatalf("Query should have been in query cache after prepared statement execution")
	}

	results, err = globalCluster.Query(query, &QueryOptions{Prepared: true})
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	var secondSamples []interface{}
	var secondSample interface{}
	for results.Next(&secondSample) {
		secondSamples = append(secondSamples, secondSample)
	}

	err = results.Close()
	if err != nil {
		t.Fatalf("results close had error: %v", err)
	}

	if len(secondSamples) != 10000 {
		t.Fatalf("Expected result to contain 10000 documents but had %d", len(secondSamples))
	}

	metadata, err = results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}

	if globalCluster.queryCache[query] == nil {
		t.Fatalf("Query should have been in query cache after prepared statement execution")
	}
}

func testQueryNamedParameters(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` where `type`=$type AND `name`=$name LIMIT 10000;"
	params := make(map[string]interface{}, 1)
	params["type"] = "hotel"
	params["name"] = "Medway Youth Hostel"
	results, err := globalCluster.Query(query, &QueryOptions{NamedParameters: params})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err != nil {
		t.Fatalf("results close had error: %v", err)
	}

	if len(samples) != 1 {
		t.Fatalf("Expected breweries to contain 1 document but had %d", len(samples))
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
	}
}

func testQueryPositionalParameters(t *testing.T) {
	query := "SELECT `travel-sample`.* FROM `travel-sample` where `type`=? AND `name`=? LIMIT 10000;"
	results, err := globalCluster.Query(query, &QueryOptions{PositionalParameters: []interface{}{"hotel", "Medway Youth Hostel"}})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err != nil {
		t.Fatalf("results close had error: %v", err)
	}

	if len(samples) != 1 {
		t.Fatalf("Expected breweries to contain 1 document but had %d", len(samples))
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata had error: %v", err)
	}

	if metadata.RequestID() == "" {
		t.Fatalf("Result should have had non empty RequestID")
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

		if len(opts) != 4 {
			t.Fatalf("Expected request body to contain 4 options but was %d, %v", len(opts), opts)
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
		optsContextID, ok := opts["client_context_id"]
		if !ok {
			t.Fatalf("Request query options missing client context id")
		}
		if optsContextID == "" {
			t.Fatalf("Client context id should have been not empty")
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

func TestBasicQuerySerializer(t *testing.T) {
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
		Serializer:           &MockSerializer{},
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

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

	res, err := cluster.Query(statement, queryOptions)
	if err != nil {
		t.Fatal(err)
	}

	var actual []byte
	var i int
	for res.Next(&actual) {
		if string(actual) != string(expectedResult.Results[i]) {
			t.Fatalf("Results did not match: expected %s but was %s", string(expectedResult.Results[i]), string(actual))
		}
		i++
	}

	err = res.Close()
	if err != nil {
		t.Fatalf("Expected error to be nil but was %v", err)
	}
}

func TestBasicQuerySerializerError(t *testing.T) {
	dataBytes, err := loadRawTestDataset("beer_sample_query_dataset")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	queryOptions := &QueryOptions{
		PositionalParameters: []interface{}{"brewery"},
		Serializer: &MockSerializer{
			err: errors.New("test error"),
		},
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

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

	res, err := cluster.Query(statement, queryOptions)
	if err != nil {
		t.Fatal(err)
	}

	var actual []byte
	for res.Next(&actual) {
		t.Fatalf("Expected no rows to be accessible")
	}

	err = res.Close()
	if err == nil {
		t.Fatalf("Expected error to be not nil")
	}
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

	_, err = cluster.Query(statement, queryOptions)
	if err == nil {
		t.Fatalf("Expected execute query to error")
	}

	queryErr, ok := err.(QueryError)
	if !ok {
		t.Fatalf("Expected error to be QueryErrors but was %s", reflect.TypeOf(err).String())
	}

	if queryErr.Endpoint() != "localhost:8093" {
		t.Fatalf("Expected error endpoint to be localhost:8093 but was %s", queryErr.Endpoint())
	}

	if queryErr.HTTPStatus() != 400 {
		t.Fatalf("Expected error HTTP status to be 400 but was %d", queryErr.HTTPStatus())
	}

	if queryErr.ContextID() != expectedResult.ClientContextID {
		t.Fatalf("Expected error ContextID to be %s but was %s", expectedResult.ClientContextID, queryErr.ContextID())
	}

	expectedErr := expectedResult.Errors[0]
	msg := fmt.Sprintf("[%d] %s", expectedErr.Code(), expectedErr.Message())

	if queryErr.Code() != expectedErr.ErrorCode {
		t.Fatalf("Expected error code to be %d but was %d", queryErr.Code(), expectedErr.ErrorCode)
	}

	if queryErr.Message() != expectedErr.ErrorMessage {
		t.Fatalf("Expected error message to be %s but was %s", queryErr.Message(), expectedErr.ErrorMessage)
	}

	if queryErr.Error() != msg {
		t.Fatalf("Expected error Error() to be %s but was %s", queryErr.Error(), msg)
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

	if !IsServiceNotAvailableError(err) {
		t.Fatalf("Expected error to be ServiceNotFoundError but was %s", reflect.TypeOf(err).Name())
	}
}

func TestQueryConnectTimeout(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 20 * time.Millisecond
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
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestQueryStreamTimeout(t *testing.T) {
	dataBytes, err := loadRawTestDataset("beer_sample_query_timeout")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 20 * time.Millisecond
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

	results, err := cluster.Query(statement, &QueryOptions{
		Timeout: timeout,
		Context: ctx,
	})
	if err != nil {
		t.Fatalf("Query shouldn't have errored but was %v", err)
	}

	var ignore interface{}
	for results.Next(&ignore) {
	}

	err = results.Close()
	if err == nil || !IsTimeoutError(err) {
		t.Fatalf("Error should have been timeout but was %v", err)
	}
}

func TestQueryConnectContextTimeout(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 50 * time.Second
	clusterTimeout := 50 * time.Second
	ctxTimeout := 10 * time.Millisecond
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
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestQueryConnectClusterTimeout(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	clusterTimeout := 10 * time.Millisecond
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

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, clusterTimeout, 0, 0)

	_, err := cluster.Query(statement, &QueryOptions{
		Context: ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func testAssertQueryRequest(t *testing.T, req *gocbcore.HttpRequest) {
	if req.Service != gocbcore.N1qlService {
		t.Fatalf("Service should have been QueryService but was %d", req.Service)
	}

	if req.Context == nil {
		t.Fatalf("Context should not have been nil, but was")
	}

	if req.Method != "POST" {
		t.Fatalf("Request method should have been POST but was %s", req.Method)
	}

	if req.Path != "/query/service" {
		t.Fatalf("Request path should have been /query/service but was %s", req.Path)
	}
}

func testAssertQueryResult(t *testing.T, expectedResult *n1qlResponse, actualResult *QueryResult, expectData bool) {
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
}

func TestBasicRetries(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	dataBytes, err := loadRawTestDataset("beer_sample_query_temp_error")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var expectedResult n1qlResponse
	err = json.Unmarshal(dataBytes, &expectedResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset %v", err)
	}

	var retries int

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)
		retries++

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 503, // this is a guess
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)
	cluster.sb.N1qlRetryBehavior = StandardDelayRetryBehavior(3, 1, 100*time.Millisecond, LinearDelayFunction)

	_, err = cluster.Query(statement, nil)
	if err == nil {
		t.Fatal("Expected query execution to error")
	}

	if retries != 3 {
		t.Fatalf("Expected query to be retried 3 time but ws retried %d times", retries)
	}
}

func TestBasicEnhancedPreparedQuery(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	dataBytes, err := loadRawTestDataset("enhanced_beer_sample_query_dataset")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var retries int

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)
		retries++

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil

	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
		supportFn: func(capability gocbcore.ClusterCapability) bool {
			return true
		},
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)
	cluster.sb.N1qlRetryBehavior = StandardDelayRetryBehavior(3, 1, 100*time.Millisecond, LinearDelayFunction)

	cluster.queryCache = map[string]*n1qlCache{
		"fake": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
		"fake2": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
	}

	_, err = cluster.Query(statement, &QueryOptions{Prepared: true})
	if err != nil {
		t.Fatalf("Expected query execution to not error %v", err)
	}

	if retries != 1 {
		t.Fatalf("Expected query to be run 1 time but was run %d times", retries)
	}

	if len(cluster.queryCache) != 1 {
		t.Fatalf("Query cache should have contained 1 item but was %v", cluster.queryCache)
	}

	cache, ok := cluster.queryCache["select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"]
	if !ok {
		t.Fatal("Expected query cache to contain query")
	}

	if cache.name != "[127.0.0.1:8091]adbf974b-9aed-5d37-9c89-3c86b69f9bd4" {
		t.Fatalf("Expected cache name to be [127.0.0.1:8091]adbf974b-9aed-5d37-9c89-3c86b69f9bd4 but was %s", cache.name)
	}

	if cache.encodedPlan != "" {
		t.Fatalf("Expected cache name to be empty but was %s", cache.encodedPlan)
	}
}

func TestBasicEnhancedPreparedQueryAlreadySupported(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	dataBytes, err := loadRawTestDataset("enhanced_beer_sample_query_dataset")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var retries int

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)
		retries++

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
		supportFn: func(capability gocbcore.ClusterCapability) bool {
			return true
		},
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)
	cluster.sb.N1qlRetryBehavior = StandardDelayRetryBehavior(3, 1, 100*time.Millisecond, LinearDelayFunction)
	cluster.supportsEnhancedStatements = 1

	cluster.queryCache = map[string]*n1qlCache{
		"fake": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
		"fake2": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
	}

	_, err = cluster.Query(statement, &QueryOptions{Prepared: true})
	if err != nil {
		t.Fatalf("Expected query execution to not error %v", err)
	}

	if retries != 1 {
		t.Fatalf("Expected query to be run 1 time but was run %d times", retries)
	}

	if len(cluster.queryCache) != 3 {
		t.Fatalf("Query cache should have contained 3 items but was %v", cluster.queryCache)
	}
}

func TestBasicEnhancedPreparedQueryAlreadyCached(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	dataBytes, err := loadRawTestDataset("beer_sample_query_dataset")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var retries int

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)
		retries++

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
		supportFn: func(capability gocbcore.ClusterCapability) bool {
			return true
		},
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)
	cluster.sb.N1qlRetryBehavior = StandardDelayRetryBehavior(3, 1, 100*time.Millisecond, LinearDelayFunction)
	cluster.supportsEnhancedStatements = 1

	cluster.queryCache = map[string]*n1qlCache{
		"fake": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
		"fake2": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
		"select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name": {
			name: "[127.0.0.1:8091]32f2405d-5715-5915-b2b2-d2c557da4996",
		},
	}

	_, err = cluster.Query(statement, &QueryOptions{Prepared: true})
	if err != nil {
		t.Fatalf("Expected query execution to not error %v", err)
	}

	if retries != 1 {
		t.Fatalf("Expected query to be run 1 times but ws run %d times", retries)
	}

	if len(cluster.queryCache) != 3 {
		t.Fatalf("Query cache should have contained 3 items but was %v", cluster.queryCache)
	}
}

func TestBasicRetriesEnhancedPreparedNoRetry(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	dataBytes, err := loadRawTestDataset("beer_sample_query_temp_error")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var expectedResult n1qlResponse
	err = json.Unmarshal(dataBytes, &expectedResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset %v", err)
	}

	var retries int

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)
		retries++

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 404,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
		supportFn: func(capability gocbcore.ClusterCapability) bool {
			return true
		},
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)
	cluster.sb.N1qlRetryBehavior = StandardDelayRetryBehavior(3, 1, 100*time.Millisecond, LinearDelayFunction)

	cluster.queryCache = map[string]*n1qlCache{
		"fake": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
		"fake2": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
	}

	_, err = cluster.Query(statement, &QueryOptions{Prepared: true})
	if err == nil {
		t.Fatal("Expected query execution to error")
	}

	if retries != 1 {
		t.Fatalf("Expected query to be retried 1 time but was retried %d times", retries)
	}

	if len(cluster.queryCache) != 0 {
		t.Fatalf("Query cache should have been empty but was %v", cluster.queryCache)
	}
}

func TestBasicRetriesEnhancedPreparedRetry(t *testing.T) {
	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	dataBytes, err := loadRawTestDataset("query_enhanced_statement_temp_error")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var expectedResult n1qlResponse
	err = json.Unmarshal(dataBytes, &expectedResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset %v", err)
	}

	var retries int

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)
		retries++

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8093",
			StatusCode: 404,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
		supportFn: func(capability gocbcore.ClusterCapability) bool {
			return true
		},
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)
	cluster.sb.N1qlRetryBehavior = StandardDelayRetryBehavior(3, 1, 100*time.Millisecond, LinearDelayFunction)

	cluster.queryCache = map[string]*n1qlCache{
		"fake": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
		"fake2": {
			name:        "mefake",
			encodedPlan: "somethingencoded",
		},
	}

	_, err = cluster.Query(statement, &QueryOptions{Prepared: true})
	if err == nil {
		t.Fatal("Expected query execution to error")
	}

	if retries != 3 {
		t.Fatalf("Expected query to be retried 3 time but ws retried %d times", retries)
	}

	if len(cluster.queryCache) != 0 {
		t.Fatalf("Query cache should have been empty but was %v", cluster.queryCache)
	}
}

func testGetClusterForHTTP(provider *mockHTTPProvider, n1qlTimeout, analyticsTimeout, searchTimeout time.Duration) *Cluster {
	clients := make(map[string]client)
	cli := &mockClient{
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: true,
		mockHTTPProvider:  provider,
	}
	clients[""] = cli
	c := &Cluster{
		connections: clients,
	}
	c.sb.QueryTimeout = n1qlTimeout
	c.sb.AnalyticsTimeout = analyticsTimeout
	c.sb.SearchTimeout = searchTimeout

	c.sb.Transcoder = NewJSONTranscoder(&DefaultJSONSerializer{})
	c.sb.Serializer = &DefaultJSONSerializer{}

	return c
}
