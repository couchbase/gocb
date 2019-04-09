package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

func TestViewQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	if globalCluster.NotSupportsFeature(ViewFeature) {
		t.Skip("Skipping test as views not supported.")
	}

	if globalTravelBucket == nil {
		t.Skip("Skipping test as no travel-sample bucket")
	}

	testCreateView(t)
	errCh := make(chan error)
	timer := time.NewTimer(30 * time.Second)
	go testWaitView(errCh)

	select {
	case <-timer.C:
		t.Fatalf("Wait time for views to become ready expired")
		close(errCh)
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Failed to wait for views to become ready: %v", err)
		}
	}

	t.Run("testSimpleViewQuery", testSimpleViewQuery)
	t.Run("testSimpleViewQueryOne", testSimpleViewQueryOne)
	t.Run("testSimpleViewQueryNone", testSimpleViewQueryNone)
	t.Run("testSimpleViewQueryOneNone", testSimpleViewQueryOneNone)
	t.Run("testSimpleViewQueryError", testSimpleViewQueryError)
	t.Run("testSimpleViewQueryOneError", testSimpleViewQueryOneError)
}

func testCreateView(t *testing.T) {
	viewFn := `function (doc, meta) {
  emit(meta.id, null);
}`
	reduceFn := `_count`
	mapStr := map[string]string{
		"map":    viewFn,
		"reduce": reduceFn,
	}
	views := map[string]map[string]string{
		"sample": mapStr,
	}
	ddoc := map[string]map[string]map[string]string{
		"views": views,
	}

	b, err := json.Marshal(ddoc)
	if err != nil {
		t.Fatalf("failed to marshal view: %v", err)
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.CapiService,
		Path:    "/_design/travel",
		Method:  "PUT",
		Body:    b,
	}

	cli := globalTravelBucket.sb.getCachedClient()

	agent, err := cli.getHTTPProvider()
	if err != nil {
		t.Fatalf("failed to get HTTP provider: %v", err)
	}

	res, err := agent.DoHttpRequest(req)
	if err != nil {
		t.Fatalf("failed to execute HTTP request: %v", err)
	}

	if res.StatusCode != http.StatusCreated {
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("failed to create view and couldn't read resp body: %v", err)
		}
		t.Fatalf("failed to create view: %s", string(data))
	}
}

func testWaitView(errCh chan error) {
	for {
		select {
		case <-errCh:
			break
		default:
		}
		time.Sleep(250 * time.Millisecond)

		results, err := globalTravelBucket.ViewQuery("travel", "sample", &ViewOptions{
			Reduce: true,
		})
		if err != nil {
			errCh <- fmt.Errorf("failed to execute query %v", err)
		}

		countMap := make(map[string]interface{})
		err = results.One(&countMap)
		if err != nil {
			continue
		}

		val, ok := countMap["value"]
		if !ok {
			errCh <- fmt.Errorf("could not get value from results")
		}

		count, ok := val.(float64)
		if !ok {
			errCh <- fmt.Errorf("could not parse results value as an int")
		}

		// We don't need to wait for all docs to become available
		if count > 10000 {
			break
		}
	}

	errCh <- nil
}

func testSimpleViewQuery(t *testing.T) {
	results, err := globalTravelBucket.ViewQuery("travel", "sample", &ViewOptions{
		Limit: 10000,
	})
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
		t.Fatalf("Metadata shouldn't have returned error, was %v", err)
	}

	if metadata.TotalRows() == 0 {
		t.Fatalf("Expected result TotalRows to be not zero")
	}
}

func testSimpleViewQueryOne(t *testing.T) {
	results, err := globalTravelBucket.ViewQuery("travel", "sample", &ViewOptions{
		Limit: 1,
	})
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
		t.Fatalf("Metadata shouldn't have returned error, was %v", err)
	}

	if metadata.TotalRows() == 0 {
		t.Fatalf("Expected result TotalRows to be not zero")
	}
}

func testSimpleViewQueryOneNone(t *testing.T) {
	results, err := globalTravelBucket.ViewQuery("travel", "sample", &ViewOptions{
		Limit: 1,
		Key:   "doesntexist",
	})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var sample interface{}
	err = results.One(&sample)
	if err == nil {
		t.Fatalf("Expected One to return error")
	}

	if err != ErrNoResults {
		t.Fatalf("Expected error to be no results but was %v", err)
	}

	if sample != nil {
		t.Fatalf("Expected sample to be nil but was %v", sample)
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata shouldn't have returned error, was %v", err)
	}

	if metadata.TotalRows() == 0 {
		t.Fatalf("Expected result TotalRows to be not zero")
	}
}

func testSimpleViewQueryOneError(t *testing.T) {
	results, err := globalTravelBucket.ViewQuery("travel", "sampl", &ViewOptions{
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var sample interface{}
	err = results.One(&sample)
	if err == nil {
		t.Fatalf("Expected One to return error")
	}

	_, ok := err.(ViewQueryErrors)
	if !ok {
		t.Fatalf("Expected error to be ViewQueryErrors but was %s", reflect.TypeOf(err).String())
	}

	if sample != nil {
		t.Fatalf("Expected sample to be nil but was %v", sample)
	}
}

func testSimpleViewQueryNone(t *testing.T) {
	results, err := globalTravelBucket.ViewQuery("travel", "sample", &ViewOptions{
		Limit: 10000,
		Key:   "idontexist",
	})
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
		t.Fatalf("Metadata shouldn't have returned error, was %v", err)
	}

	if metadata.TotalRows() == 0 {
		t.Fatalf("Expected result TotalRows to be not zero")
	}
}

func testSimpleViewQueryError(t *testing.T) {
	results, err := globalTravelBucket.ViewQuery("travel", "sampl", &ViewOptions{
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("Failed to execute query %v", err)
	}

	var samples []interface{}
	var sample interface{}
	for results.Next(&sample) {
		samples = append(samples, sample)
	}

	err = results.Close()
	if err == nil {
		t.Fatalf("Expected results close should to have error")
	}

	_, ok := err.(ViewQueryErrors)
	if !ok {
		t.Fatalf("Expected error to be ViewQueryErrors but was %s", reflect.TypeOf(err).String())
	}

	if len(samples) != 0 {
		t.Fatalf("Expected result to contain 0 documents but had %d", len(samples))
	}
}

func TestViewQuery500Error(t *testing.T) {
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertViewQueryRequest(t, req)

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8092",
			StatusCode: 500,
			Body:       &testReadCloser{bytes.NewBuffer([]byte("[\"Unexpected server error, request logged.\"]")), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	bucket := testGetBucketForHTTP(provider, timeout)

	res, err := bucket.ViewQuery("test", "test", nil)
	if err != nil {
		t.Fatalf("Expected query to not return error but was %v", err)
	}

	if res == nil {
		t.Fatal("Expected result to be not nil but was")
	}

	var results []interface{}
	var row interface{}
	for res.Next(&row) {
		results = append(results, row)
	}

	if len(results) > 0 {
		t.Fatalf("results should have had length 0 but was %d", len(results))
	}

	err = res.Close()
	if err == nil {
		t.Fatalf("results close should have errored")
	}

	queryErrs, ok := err.(ViewQueryErrors)
	if !ok {
		t.Fatalf("Expected error to be ViewQueryErrors but was %s", reflect.TypeOf(err).String())
	}

	if queryErrs.Endpoint() != "http://localhost:8092" {
		t.Fatalf("Expected error endpoint to be http://localhost:8092 but was %s", queryErrs.Endpoint())
	}

	if queryErrs.HTTPStatus() != 500 {
		t.Fatalf("Expected error HTTP status to be 500 but was %d", queryErrs.HTTPStatus())
	}

	if len(queryErrs.Errors()) != 1 {
		t.Fatalf("Expected errors to contain 1 error but contained %d", len(queryErrs.Errors()))
	}

	queryErr := queryErrs.Errors()[0]
	msg := fmt.Sprintf("%s - %s", "Unexpected server error, request logged.", "500")

	if queryErr.Message() != "Unexpected server error, request logged." {
		t.Fatalf("Expected error message to be \"Unexpected server error, request logged.\" but was %s", queryErr.Message())
	}

	if queryErr.Reason() != "500" {
		t.Fatalf("Expected error reason to be 500 but was %s", queryErr.Reason())
	}

	if queryErr.Error() != msg {
		t.Fatalf("Expected error Error() to be %s but was %s", msg, queryErr.Error())
	}

	if queryErrs.Error() != msg {
		t.Fatalf("Expected errors Error() to be %s but was %s", msg, queryErrs.Error())
	}
}

func TestViewServiceNotFound(t *testing.T) {
	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		return nil, gocbcore.ErrNoCapiService
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	bucket := testGetBucketForHTTP(provider, 10*time.Second)

	res, err := bucket.ViewQuery("test", "test", nil)
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

func testAssertViewQueryRequest(t *testing.T, req *gocbcore.HttpRequest) {
	if req.Service != gocbcore.CapiService {
		t.Fatalf("Service should have been N1qlService but was %d", req.Service)
	}

	if req.Context == nil {
		t.Fatalf("Context should not have been nil, but was")
	}

	if req.Method != "GET" {
		t.Fatalf("Request method should have been GET but was %s", req.Method)
	}
}

func testGetBucketForHTTP(provider *mockHTTPProvider, viewTimeout time.Duration) *Bucket {
	clients := make(map[string]client)
	cli := &mockClient{
		bucketName:        "mock",
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: false,
		mockHTTPProvider:  provider,
	}
	clients["mock-false"] = cli
	c, _ := Connect("couchbase://localhost", ClusterOptions{
		ViewTimeout: viewTimeout,
	})
	c.connections = clients
	b := c.Bucket("mock", nil)
	b.sb.ViewTimeout = viewTimeout

	return b
}
