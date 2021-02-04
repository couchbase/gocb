package gocb

import (
	"context"
	"time"
)

func (suite *UnitTestSuite) TestThresholdGroup() {
	time.Sleep(100 * time.Millisecond)

	grp := initThresholdLogGroup("Test", 2*time.Millisecond, 3)
	grp.recordOp(&thresholdLogSpan{duration: 1 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 2 * time.Millisecond})

	if len(grp.ops) != 1 {
		suite.T().Fatalf("Failed to ignore duration below threshold")
	}

	grp.recordOp(&thresholdLogSpan{duration: 6 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 4 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 5 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 2 * time.Millisecond})
	grp.recordOp(&thresholdLogSpan{duration: 9 * time.Millisecond})

	if len(grp.ops) != 3 {
		suite.T().Fatalf("Failed to reach real capacity")
	}
	if grp.ops[0].duration != 5*time.Millisecond {
		suite.T().Fatalf("Failed to insert in correct order (1)")
	}
	if grp.ops[1].duration != 6*time.Millisecond {
		suite.T().Fatalf("Failed to insert in correct order (2)")
	}
	if grp.ops[2].duration != 9*time.Millisecond {
		suite.T().Fatalf("Failed to insert in correct order (3)")
	}
}

func (suite *UnitTestSuite) TestThresholdLogger() {
	logger := NewThresholdLoggingTracer(&ThresholdLoggingOptions{
		KVThreshold:    1,
		QueryThreshold: 1,
	})

	span1 := logger.RequestSpan(context.Background(), "Set")

	span1.SetAttribute(spanAttribDBNameKey, "mybucket")
	span1.SetAttribute(spanAttribDBCollectionNameKey, "mycollection")
	span1.SetAttribute(spanAttribDBScopeNameKey, "myscope")
	span1.SetAttribute(spanAttribServiceKey, "kv")
	span1.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)

	span1Encode := logger.RequestSpan(span1.Context(), spanNameRequestEncoding)
	span1Encode.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
	time.Sleep(50 * time.Microsecond)
	span1Encode.End()

	span1Dispatch := logger.RequestSpan(span1.Context(), spanNameDispatchToServer)

	span1Dispatch.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
	span1Dispatch.SetAttribute(spanAttribOperationIDKey, "1")
	span1Dispatch.SetAttribute(spanAttribLocalIDKey, "12345")
	span1Dispatch.SetAttribute(spanAttribNetHostNameKey, "localhost")
	span1Dispatch.SetAttribute(spanAttribNetHostPortKey, "5431")
	span1Dispatch.SetAttribute(spanAttribNetPeerNameKey, "remotehost")
	span1Dispatch.SetAttribute(spanAttribNetPeerPortKey, "11210")
	span1Dispatch.SetAttribute(spanAttribServerDurationKey, 1100*time.Microsecond)

	time.Sleep(50 * time.Microsecond)
	span1Dispatch.End()
	span1.End()

	span2 := logger.RequestSpan(context.Background(), "query")

	span2.SetAttribute(spanAttribServiceKey, "query")
	span2.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)

	span2Encode := logger.RequestSpan(span2.Context(), spanNameRequestEncoding)
	span2Encode.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
	time.Sleep(50 * time.Microsecond)
	span2Encode.End()

	span2Dispatch := logger.RequestSpan(span2.Context(), spanNameDispatchToServer)
	span2Dispatch.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
	span2Dispatch.SetAttribute(spanAttribOperationIDKey, "clientcontextid")
	span2Dispatch.SetAttribute(spanAttribNetPeerNameKey, "remotehost")
	span2Dispatch.SetAttribute(spanAttribNetPeerPortKey, "8093")

	time.Sleep(50 * time.Microsecond)
	span2Dispatch.End()
	span2.End()

	data := logger.buildJSONData()

	suite.Assert().Len(data, 2)
	if suite.Assert().Contains(data, "kv") {
		kvData := data["kv"]
		suite.Assert().Equal(uint64(1), kvData.Count)
		top := kvData.Top
		suite.Assert().Equal(1, len(top))
		item := top[0]
		suite.Assert().Equal("Set", item.OperationName)
		suite.Assert().Equal("localhost:5431", item.LastLocalAddress)
		suite.Assert().Equal(uint64(1100), item.LastServerDurationUs)
		suite.Assert().NotZero(item.DispatchDurationUs)
		suite.Assert().NotZero(item.EncodeDurationUs)
		suite.Assert().NotZero(item.LastDispatchDurationUs)
		suite.Assert().Equal("12345", item.LastLocalID)
		suite.Assert().Equal("1", item.LastOperationID)
		suite.Assert().Equal("remotehost:11210", item.LastRemoteAddress)
		suite.Assert().Equal(uint64(1100), item.ServerDurationUs)
		suite.Assert().NotZero(item.TotalTimeUs)
	}
	if suite.Assert().Contains(data, "query") {
		queryData := data["query"]
		suite.Assert().Equal(uint64(1), queryData.Count)
		top := queryData.Top
		suite.Assert().Equal(1, len(top))
		item := top[0]
		suite.Assert().Equal("query", item.OperationName)
		suite.Assert().Empty(item.LastLocalAddress)
		suite.Assert().Empty(item.LastServerDurationUs)
		suite.Assert().NotZero(item.DispatchDurationUs)
		suite.Assert().NotZero(item.EncodeDurationUs)
		suite.Assert().NotZero(item.LastDispatchDurationUs)
		suite.Assert().Equal("clientcontextid", item.LastOperationID)
		suite.Assert().Equal("remotehost:8093", item.LastRemoteAddress)
		suite.Assert().NotZero(item.TotalTimeUs)
	}
}
