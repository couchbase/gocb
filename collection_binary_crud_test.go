package gocb

import "github.com/couchbase/gocbcore/v9/memd"

func (suite *IntegrationTestSuite) TestBinaryAppend() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(AdjoinFeature)
	colBinary := globalCollection.Binary()

	tcoder := NewRawBinaryTranscoder()
	res, err := globalCollection.Upsert("binaryAppend", []byte("foo"), &UpsertOptions{
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Upsert, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	appendRes, err := colBinary.Append("binaryAppend", []byte("bar"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to Append, err: %v", err)
	}

	if appendRes.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	appendDoc, err := globalCollection.Get("binaryAppend", &GetOptions{
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var appendContent []byte
	err = appendDoc.Content(&appendContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if string(appendContent) != "foobar" {
		suite.T().Fatalf("Expected append result to be foobar but was %s", appendContent)
	}

	suite.Require().Contains(globalTracer.Spans, nil)
	nilParents := globalTracer.Spans[nil]
	suite.Require().Equal(len(nilParents), 3)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "append", memd.CmdAppend.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "append", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
}

func (suite *IntegrationTestSuite) TestBinaryPrepend() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(AdjoinFeature)

	colBinary := globalCollection.Binary()

	tcoder := NewRawBinaryTranscoder()
	res, err := globalCollection.Upsert("binaryPrepend", []byte("foo"), &UpsertOptions{
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Upsert, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	appendRes, err := colBinary.Prepend("binaryPrepend", []byte("bar"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to Append, err: %v", err)
	}

	if appendRes.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	appendDoc, err := globalCollection.Get("binaryPrepend", &GetOptions{
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var appendContent []byte
	err = appendDoc.Content(&appendContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if string(appendContent) != "barfoo" {
		suite.T().Fatalf("Expected prepend result to be boofar but was %s", appendContent)
	}

	suite.Require().Contains(globalTracer.Spans, nil)
	nilParents := globalTracer.Spans[nil]
	suite.Require().Equal(len(nilParents), 3)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "prepend", memd.CmdPrepend.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "prepend", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
}

func (suite *IntegrationTestSuite) TestBinaryIncrement() {
	suite.skipIfUnsupported(KeyValueFeature)

	colBinary := globalCollection.Binary()

	res, err := colBinary.Increment("binaryIncrement", &IncrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 0 {
		suite.T().Fatalf("Expected counter value to be 0 but was %d", res.Content())
	}

	res, err = colBinary.Increment("binaryIncrement", &IncrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 10 {
		suite.T().Fatalf("Expected counter value to be 10 but was %d", res.Content())
	}

	res, err = colBinary.Increment("binaryIncrement", &IncrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 20 {
		suite.T().Fatalf("Expected counter value to be 20 but was %d", res.Content())
	}

	incrementDoc, err := globalCollection.Get("binaryIncrement", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var incrementContent int
	err = incrementDoc.Content(&incrementContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if incrementContent != 20 {
		suite.T().Fatalf("Expected counter value to be 20 but was %d", res.Content())
	}

	suite.Require().Contains(globalTracer.Spans, nil)
	nilParents := globalTracer.Spans[nil]
	suite.Require().Equal(len(nilParents), 4)
	suite.AssertKvOpSpan(nilParents[0], "increment", memd.CmdIncrement.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "increment", memd.CmdIncrement.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "increment", memd.CmdIncrement.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[3], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "increment", 3, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
}

func (suite *IntegrationTestSuite) TestBinaryDecrement() {
	suite.skipIfUnsupported(KeyValueFeature)

	colBinary := globalCollection.Binary()

	res, err := colBinary.Decrement("binaryDecrement", &DecrementOptions{
		Delta:   10,
		Initial: 100,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 100 {
		suite.T().Fatalf("Expected counter value to be 100 but was %d", res.Content())
	}

	res, err = colBinary.Decrement("binaryDecrement", &DecrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 90 {
		suite.T().Fatalf("Expected counter value to be 90 but was %d", res.Content())
	}

	res, err = colBinary.Decrement("binaryDecrement", &DecrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 80 {
		suite.T().Fatalf("Expected counter value to be 80 but was %d", res.Content())
	}

	incrementDoc, err := globalCollection.Get("binaryDecrement", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var incrementContent int
	err = incrementDoc.Content(&incrementContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if incrementContent != 80 {
		suite.T().Fatalf("Expected counter value to be 80 but was %d", res.Content())
	}

	suite.Require().Contains(globalTracer.Spans, nil)
	nilParents := globalTracer.Spans[nil]
	suite.Require().Equal(len(nilParents), 4)
	suite.AssertKvOpSpan(nilParents[0], "decrement", memd.CmdDecrement.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "decrement", memd.CmdDecrement.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "decrement", memd.CmdDecrement.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[3], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "decrement", 3, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
}
