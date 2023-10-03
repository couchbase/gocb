package gocb

import (
	"sync"
	"time"
)

type testSpan struct {
	Name          string
	Tags          map[string]interface{}
	Finished      bool
	ParentContext RequestSpanContext
	Spans         map[RequestSpanContext][]*testSpan
}

func (ts *testSpan) End() {
	ts.Finished = true
}

func (ts *testSpan) Context() RequestSpanContext {
	return ts.Spans
}

func newTestSpan(operationName string, parentContext RequestSpanContext) *testSpan {
	return &testSpan{
		Name:          operationName,
		Tags:          make(map[string]interface{}),
		ParentContext: parentContext,
		Spans:         make(map[RequestSpanContext][]*testSpan),
	}
}

func (ts *testSpan) SetAttribute(key string, value interface{}) {
	ts.Tags[key] = value
}

func (ts *testSpan) AddEvent(key string, timestamp time.Time) {
}

type testTracer struct {
	Spans map[RequestSpanContext][]*testSpan
	lock  sync.Mutex
}

func newTestTracer() *testTracer {
	return &testTracer{
		Spans: make(map[RequestSpanContext][]*testSpan),
	}
}

func (tt *testTracer) RequestSpan(parentContext RequestSpanContext, operationName string) RequestSpan {
	span := newTestSpan(operationName, parentContext)
	tt.lock.Lock()
	if parentContext == nil {
		tt.Spans[parentContext] = append(tt.Spans[parentContext], span)
	} else {
		ctx, ok := parentContext.(map[RequestSpanContext][]*testSpan)
		if ok {
			ctx[operationName] = append(ctx[operationName], span)
		} else {
			tt.Spans[parentContext] = append(tt.Spans[parentContext], span)
		}
	}
	tt.lock.Unlock()

	return span
}

func (tt *testTracer) Reset() {
	tt.lock.Lock()
	tt.Spans = make(map[RequestSpanContext][]*testSpan)
	tt.lock.Unlock()
}

func (tt *testTracer) GetSpans() map[RequestSpanContext][]*testSpan {
	spans := make(map[RequestSpanContext][]*testSpan)
	tt.lock.Lock()
	for ctx, ttSpans := range tt.Spans {
		// The underlying spans won't change but the list at the top level itself could do.
		thisSpans := make([]*testSpan, len(ttSpans))
		for i, span := range ttSpans {
			thisSpans[i] = span
		}
		spans[ctx] = thisSpans
	}
	tt.lock.Unlock()

	return spans
}

func (suite *IntegrationTestSuite) AssertKvOpSpan(span *testSpan, opName, cmdName string, hasEncoding bool, durability DurabilityLevel) {
	suite.AssertKvSpan(span, opName, durability)

	if hasEncoding {
		suite.AssertEncodingSpansEq(span.Spans, 1)
	}

	suite.AssertCmdSpans(span.Spans, cmdName)
}

type HTTPOpSpanExpectations struct {
	bucket                  string
	scope                   string
	collection              string
	service                 string
	operationID             string
	numDispatchSpans        int
	atLeastNumDispatchSpans bool
	hasEncoding             bool
	dispatchOperationID     string
	statement               string
}

func (suite *IntegrationTestSuite) AssertHTTPOpSpan(span *testSpan, opName string, expectations HTTPOpSpanExpectations) {
	suite.AssertHTTPSpan(span, opName, expectations.bucket, expectations.scope, expectations.collection, expectations.service,
		expectations.operationID, expectations.statement)

	if expectations.hasEncoding {
		suite.AssertEncodingSpansEq(span.Spans, 1)
	}

	if expectations.atLeastNumDispatchSpans {
		suite.AssertHTTPDispatchSpansGE(span.Spans, expectations.numDispatchSpans, expectations.dispatchOperationID)
	} else {
		suite.AssertHTTPDispatchSpansEQ(span.Spans, expectations.numDispatchSpans, expectations.dispatchOperationID)
	}
}

func (suite *IntegrationTestSuite) RequireQueryMgmtOpSpan(span *testSpan, opName, childType string) *testSpan {
	suite.Assert().Equal(opName, span.Name)
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	suite.Assert().Equal("management", span.Tags["db.couchbase.service"])
	suite.Require().Len(span.Spans, 1)
	suite.Require().Len(span.Spans[childType], 1)

	return span.Spans[childType][0]
}

func (suite *IntegrationTestSuite) AssertKvSpan(span *testSpan, expectedName string, durability DurabilityLevel) {
	scope := globalConfig.Scope
	if scope == "" {
		scope = "_default"
	}
	col := globalConfig.Collection
	if col == "" {
		col = "_default"
	}
	suite.Assert().Equal(expectedName, span.Name)

	numTags := 6
	if durability > DurabilityLevelNone {
		numTags = 7
	}

	suite.Assert().Equal(numTags, len(span.Tags))
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	suite.Assert().Equal(globalConfig.Bucket, span.Tags["db.name"])
	suite.Assert().Equal(scope, span.Tags["db.couchbase.scope"])
	suite.Assert().Equal(col, span.Tags["db.couchbase.collection"])
	suite.Assert().Equal("kv", span.Tags["db.couchbase.service"])
	suite.Assert().Equal(expectedName, span.Tags["db.operation"])
	if durability == DurabilityLevelNone {
		suite.Assert().NotContains(span.Tags, "db.couchbase.durability")
	} else {
		if duraLevel, err := durability.toManagementAPI(); err == nil {
			suite.Assert().Equal(duraLevel, span.Tags["db.couchbase.durability"])
		} else {
			logDebugf("Failed to get durability level: %v", err)
		}
	}
	suite.Assert().True(span.Finished)
}

func (suite *IntegrationTestSuite) AssertHTTPSpan(span *testSpan, name, bucket, scope, collection, service, op, statement string) {
	suite.Assert().Equal(name, span.Name)
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	suite.Assert().Equal(service, span.Tags["db.couchbase.service"])
	spans := 2
	if op == "" {
		suite.Assert().NotContains(span.Tags, "db.operation")
	} else {
		spans++
		suite.Assert().Equal(op, span.Tags["db.operation"])
	}
	if bucket == "" {
		suite.Assert().NotContains(span.Tags, "db.name")
	} else {
		spans++
		suite.Assert().Equal(globalConfig.Bucket, span.Tags["db.name"])
	}
	if scope == "" {
		suite.Assert().NotContains(span.Tags, "db.couchbase.scope")
	} else {
		spans++
		suite.Assert().Equal(scope, span.Tags["db.couchbase.scope"])
	}
	if collection == "" {
		suite.Assert().NotContains(span.Tags, "db.couchbase.collection")
	} else {
		spans++
		suite.Assert().Equal(collection, span.Tags["db.couchbase.collection"])
	}
	if statement == "" {
		suite.Assert().NotContains(span.Tags, "db.statement")
	} else if statement == "any" {
		spans++
		suite.Assert().NotEmpty(span.Tags["db.statement"])
	} else {
		spans++
		suite.Assert().Equal(statement, span.Tags["db.statement"])
	}
	suite.Assert().Equal(spans, len(span.Tags))
	suite.Assert().True(span.Finished)
}

func (suite *IntegrationTestSuite) AssertEncodingSpansEq(parents map[RequestSpanContext][]*testSpan, num int) {
	if suite.Assert().Contains(parents, "request_encoding") {
		spans := parents["request_encoding"]
		if suite.Assert().Equal(num, len(spans)) {
			for i := 0; i < num; i++ {
				suite.AssertEncodingSpan(spans[i])
			}

		}
	}
}

func (suite *IntegrationTestSuite) AssertEncodingSpan(span *testSpan) {
	suite.Assert().Equal("request_encoding", span.Name)
	suite.Assert().Equal(1, len(span.Tags))
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	suite.Assert().True(span.Finished)
}

func (suite *IntegrationTestSuite) AssertCmdSpans(parents map[RequestSpanContext][]*testSpan, cmdName string) {
	spans := parents[cmdName]
	for i := 0; i < len(spans); i++ {
		suite.AssertCmdSpan(spans[i], cmdName)
	}
}

func (suite *IntegrationTestSuite) AssertCmdSpan(span *testSpan, expectedName string) {
	suite.Assert().Equal(expectedName, span.Name)
	suite.Assert().Equal(2, len(span.Tags))
	suite.Assert().True(span.Finished)
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	suite.Assert().Contains(span.Tags, "db.couchbase.retries")

	suite.AssertKVDispatchSpans(span.Spans)
}

func (suite *IntegrationTestSuite) AssertKVDispatchSpans(parents map[RequestSpanContext][]*testSpan) {
	spans := parents["dispatch_to_server"]
	for i := 0; i < len(spans); i++ {
		suite.AssertKVDispatchSpan(spans[i])
	}
}

func (suite *IntegrationTestSuite) AssertHTTPDispatchSpansEQ(parents map[RequestSpanContext][]*testSpan, num int, operationID string) {
	spans := parents["dispatch_to_server"]
	if suite.Assert().Equal(num, len(spans)) {
		for i := 0; i < len(spans); i++ {
			suite.AssertHTTPDispatchSpan(spans[i], operationID)
		}
	}
}

func (suite *IntegrationTestSuite) AssertHTTPDispatchSpansGE(parents map[RequestSpanContext][]*testSpan, num int, operationID string) {
	spans := parents["dispatch_to_server"]
	if suite.Assert().GreaterOrEqual(num, len(spans)) {
		for i := 0; i < len(spans); i++ {
			suite.AssertHTTPDispatchSpan(spans[i], operationID)
		}
	}
}

func (suite *IntegrationTestSuite) AssertKVDispatchSpan(span *testSpan) {
	suite.Assert().Equal("dispatch_to_server", span.Name)
	suite.Assert().Equal(9, len(span.Tags))
	suite.Assert().True(span.Finished)
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	suite.Assert().Equal("IP.TCP", span.Tags["net.transport"])
	suite.Assert().NotEmpty(span.Tags["db.couchbase.operation_id"])
	suite.Assert().NotEmpty(span.Tags["db.couchbase.local_id"])
	suite.Assert().NotEmpty(span.Tags["net.host.name"])
	suite.Assert().NotEmpty(span.Tags["net.host.port"])
	suite.Assert().NotEmpty(span.Tags["net.peer.name"])
	suite.Assert().NotEmpty(span.Tags["net.peer.port"])
	suite.Assert().Contains(span.Tags, "db.couchbase.server_duration")
}

func (suite *IntegrationTestSuite) AssertHTTPDispatchSpan(span *testSpan, operationID string) {
	suite.Assert().Equal("dispatch_to_server", span.Name)
	suite.Assert().True(span.Finished)
	suite.Assert().Equal("couchbase", span.Tags["db.system"])
	suite.Assert().Equal("IP.TCP", span.Tags["net.transport"])
	spans := 3
	if !globalCluster.IsProtostellar() {
		spans += 2
		suite.Assert().NotEmpty(span.Tags["net.peer.name"])
		suite.Assert().NotEmpty(span.Tags["net.peer.port"])
	}
	if operationID == "" {
		suite.Assert().NotContains(span.Tags, "db.couchbase.operation_id")
	} else if operationID == "any" {
		spans++
		suite.Assert().NotEmpty(span.Tags["db.couchbase.operation_id"])
	} else {
		spans++
		suite.Assert().Equal(operationID, span.Tags["db.couchbase.operation_id"])
	}
	suite.Assert().Equal(spans, len(span.Tags))
}
