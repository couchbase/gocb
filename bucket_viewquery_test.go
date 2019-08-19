package gocb

import (
	"bytes"
	"context"
	"fmt"
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

	testCreateView(t, 30*time.Second)
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
}

func testCreateView(t *testing.T, timeout time.Duration) {
	viewFn := `function (doc, meta) {
  emit(meta.id, 1);
}`
	reduceFn := `_count`
	view := View{
		Map:    viewFn,
		Reduce: reduceFn,
	}
	views := map[string]View{
		"sample": view,
	}

	mgr, err := globalTravelBucket.ViewIndexes()
	if err != nil {
		t.Fatalf("Failed to get view index manager: %v", err)
	}

	err = mgr.UpsertDesignDocument(DesignDocument{
		Views: views,
		Name:  "travel",
	}, DevelopmentDesignDocumentNamespace, &UpsertDesignDocumentOptions{
		Timeout: timeout,
	})
	if err != nil {
		t.Fatalf("Failed to create design document: %v", err)
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

		var row ViewRow
		err = results.One(&row)
		if err != nil {
			continue
		}

		var count int
		err = row.Value(&count)
		if err != nil {
			errCh <- fmt.Errorf("could not get count from results %v", err)
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
	var row ViewRow
	for results.Next(&row) {
		var sample interface{}
		err = row.Value(&sample)
		if err != nil {
			t.Fatalf("Failed to Value result: %v", err)
		}
		if row.ID == "" {
			t.Fatalf("Metadata ID should not have been empty")
		}
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

	var row ViewRow
	err = results.One(&row)
	if err != nil {
		t.Fatalf("Reading row had error: %v", err)
	}
	if row.ID == "" {
		t.Fatalf("Metadata ID should not have been empty")
	}

	var sample interface{}
	err = row.Value(&sample)
	if err != nil {
		t.Fatalf("Failed to Value result: %v", err)
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

	var row ViewRow
	err = results.One(&row)
	if err == nil {
		t.Fatalf("Expected One to return error")
	}

	if !IsNoResultsError(err) {
		t.Fatalf("Expected error to be no results but was %v", err)
	}

	metadata, err := results.Metadata()
	if err != nil {
		t.Fatalf("Metadata shouldn't have returned error, was %v", err)
	}

	if metadata.TotalRows() == 0 {
		t.Fatalf("Expected result TotalRows to be not zero")
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
	var row ViewRow
	for results.Next(&row) {
		var sample interface{}
		err = row.Value(&sample)
		if err != nil {
			t.Fatalf("Failed to Value result: %v", err)
		}
		if row.ID == "" {
			t.Fatalf("Metadata ID should not have been empty")
		}
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
	_, err := globalTravelBucket.ViewQuery("travel", "sampl", &ViewOptions{
		Limit: 1,
	})
	if err == nil {
		t.Fatalf("Expected execute query to error")
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

	_, err := bucket.ViewQuery("test", "test", nil)
	if err == nil {
		t.Fatalf("Expected query to return error")
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

	if !IsServiceNotAvailableError(err) {
		t.Fatalf("Expected error to be ServiceNotFoundError but was %s", reflect.TypeOf(err).Name())
	}
}

func TestViewQueryTimeout(t *testing.T) {
	timeout := 20 * time.Millisecond
	clusterTimeout := 50 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertViewQueryRequest(t, req)

		d, ok := req.Context.Deadline()
		if !ok {
			t.Fatal("Expected request to have a deadline")
		}

		dur := d.Sub(time.Now())
		if dur < (timeout-50*time.Millisecond) || dur > (timeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", timeout.String(), dur.String())
		}

		// we can't use time travel here as we need the context to actually timeout
		time.Sleep(100 * time.Millisecond)

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	bucket := testGetBucketForHTTP(provider, clusterTimeout)

	_, err := bucket.ViewQuery("test", "test", &ViewOptions{
		Timeout: timeout,
		Context: ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestViewQueryContextTimeout(t *testing.T) {
	timeout := 2000 * time.Millisecond
	clusterTimeout := 50 * time.Second
	ctxTimeout := 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertViewQueryRequest(t, req)

		d, ok := req.Context.Deadline()
		if !ok {
			t.Fatal("Expected request to have a deadline")
		}

		dur := d.Sub(time.Now())
		if dur < (ctxTimeout-50*time.Millisecond) || dur > (ctxTimeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", ctxTimeout.String(), dur.String())
		}

		// we can't use time travel here as we need the context to actually timeout
		time.Sleep(100 * time.Millisecond)

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	bucket := testGetBucketForHTTP(provider, clusterTimeout)

	_, err := bucket.ViewQuery("test", "test", &ViewOptions{
		Timeout: timeout,
		Context: ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestViewQueryContextTimeoutNoTimeoutValue(t *testing.T) {
	clusterTimeout := 50 * time.Second
	ctxTimeout := 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertViewQueryRequest(t, req)

		d, ok := req.Context.Deadline()
		if !ok {
			t.Fatal("Expected request to have a deadline")
		}

		dur := d.Sub(time.Now())
		if dur < (ctxTimeout-50*time.Millisecond) || dur > (ctxTimeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", ctxTimeout.String(), dur.String())
		}

		// we can't use time travel here as we need the context to actually timeout
		time.Sleep(100 * time.Millisecond)

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	bucket := testGetBucketForHTTP(provider, clusterTimeout)

	_, err := bucket.ViewQuery("test", "test", &ViewOptions{
		Context: ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func TestViewQueryClusterTimeoutNoTimeoutValue(t *testing.T) {
	clusterTimeout := 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2000*time.Millisecond)
	defer cancel()

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertViewQueryRequest(t, req)

		d, ok := req.Context.Deadline()
		if !ok {
			t.Fatal("Expected request to have a deadline")
		}

		dur := d.Sub(time.Now())
		if dur < (clusterTimeout-50*time.Millisecond) || dur > (clusterTimeout+50*time.Millisecond) {
			t.Fatalf("Expected timeout to be %s but was %s", clusterTimeout.String(), dur.String())
		}

		// we can't use time travel here as we need the context to actually timeout
		time.Sleep(100 * time.Millisecond)

		return nil, context.DeadlineExceeded
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	bucket := testGetBucketForHTTP(provider, clusterTimeout)

	_, err := bucket.ViewQuery("test", "test", &ViewOptions{
		Context: ctx,
	})
	if err == nil || !IsTimeoutError(err) {
		t.Fatal(err)
	}
}

func testAssertViewQueryRequest(t *testing.T, req *gocbcore.HttpRequest) {
	if req.Service != gocbcore.CapiService {
		t.Fatalf("Service should have been QueryService but was %d", req.Service)
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
