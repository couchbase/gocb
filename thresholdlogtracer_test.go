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

func (suite *UnitTestSuite) runThresholdLoggerTest(includeLegacy, includeStable bool) {
	logger := NewThresholdLoggingTracer(&ThresholdLoggingOptions{
		KVThreshold:    1,
		QueryThreshold: 1,
	})

	span1 := logger.RequestSpan(context.Background(), "Set")

	if includeLegacy {
		span1.SetAttribute(spanLegacyAttribBucketName, "mybucket")
		span1.SetAttribute(spanLegacyAttribCollectionName, "mycollection")
		span1.SetAttribute(spanLegacyAttribScopeName, "myscope")
		span1.SetAttribute(spanLegacyAttribService, "kv")
		span1.SetAttribute(spanLegacyAttribSystemName, spanAttribSystemNameValue)
	}
	if includeStable {
		span1.SetAttribute(spanStableAttribBucketName, "mybucket")
		span1.SetAttribute(spanStableAttribCollectionName, "mycollection")
		span1.SetAttribute(spanStableAttribScopeName, "myscope")
		span1.SetAttribute(spanStableAttribService, "kv")
		span1.SetAttribute(spanStableAttribSystemName, spanAttribSystemNameValue)
	}

	span1Encode := logger.RequestSpan(span1.Context(), spanNameRequestEncoding)
	if includeLegacy {
		span1Encode.SetAttribute(spanLegacyAttribSystemName, spanAttribSystemNameValue)
	}
	if includeStable {
		span1Encode.SetAttribute(spanStableAttribSystemName, spanAttribSystemNameValue)
	}
	time.Sleep(50 * time.Microsecond)
	span1Encode.End()

	span1Dispatch := logger.RequestSpan(span1.Context(), spanNameDispatchToServer)

	if includeLegacy {
		span1Dispatch.SetAttribute(spanLegacyAttribSystemName, spanAttribSystemNameValue)
		span1Dispatch.SetAttribute(spanLegacyAttribOperationID, "1")
		span1Dispatch.SetAttribute(spanLegacyAttribLocalID, "12345")
		span1Dispatch.SetAttribute(spanLegacyAttribNetHostName, "localhost")
		span1Dispatch.SetAttribute(spanLegacyAttribNetHostPort, "5431")
		span1Dispatch.SetAttribute(spanLegacyAttribNetPeerName, "remotehost")
		span1Dispatch.SetAttribute(spanLegacyAttribNetPeerPort, "11210")
		span1Dispatch.SetAttribute(spanLegacyAttribServerDuration, 1100*time.Microsecond)
	}
	if includeStable {
		span1Dispatch.SetAttribute(spanStableAttribSystemName, spanAttribSystemNameValue)
		span1Dispatch.SetAttribute(spanStableAttribOperationID, "1")
		span1Dispatch.SetAttribute(spanStableAttribLocalID, "12345")
		span1Dispatch.SetAttribute(spanStableAttribNetPeerAddress, "remotehost")
		span1Dispatch.SetAttribute(spanStableAttribNetPeerPort, "11210")
		span1Dispatch.SetAttribute(spanLegacyAttribServerDuration, 1100*time.Microsecond)
	}

	time.Sleep(50 * time.Microsecond)
	span1Dispatch.End()
	span1.End()

	span2 := logger.RequestSpan(context.Background(), "query")

	if includeLegacy {
		span2.SetAttribute(spanLegacyAttribService, "query")
		span2.SetAttribute(spanLegacyAttribSystemName, spanAttribSystemNameValue)
	}
	if includeStable {
		span2.SetAttribute(spanStableAttribService, "query")
		span2.SetAttribute(spanStableAttribSystemName, spanAttribSystemNameValue)
	}

	span2Encode := logger.RequestSpan(span2.Context(), spanNameRequestEncoding)
	if includeLegacy {
		span2Encode.SetAttribute(spanLegacyAttribSystemName, spanAttribSystemNameValue)
	}
	if includeStable {
		span2Encode.SetAttribute(spanStableAttribSystemName, spanAttribSystemNameValue)
	}
	time.Sleep(50 * time.Microsecond)
	span2Encode.End()

	span2Dispatch := logger.RequestSpan(span2.Context(), spanNameDispatchToServer)

	if includeLegacy {
		span2Dispatch.SetAttribute(spanLegacyAttribSystemName, spanAttribSystemNameValue)
		span2Dispatch.SetAttribute(spanLegacyAttribOperationID, "clientcontextid")
		span2Dispatch.SetAttribute(spanLegacyAttribNetPeerName, "remotehost")
		span2Dispatch.SetAttribute(spanLegacyAttribNetPeerPort, "8093")
	}
	if includeStable {
		span2Dispatch.SetAttribute(spanStableAttribSystemName, spanAttribSystemNameValue)
		span2Dispatch.SetAttribute(spanStableAttribOperationID, "clientcontextid")
		span2Dispatch.SetAttribute(spanStableAttribNetPeerAddress, "remotehost")
		span2Dispatch.SetAttribute(spanStableAttribNetPeerPort, "8093")
	}

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
		if includeLegacy {
			suite.Assert().Equal("localhost:5431", item.LastLocalAddress)
		} else {
			// No host address/port in stable semantic conventions
			suite.Assert().Empty(item.LastLocalAddress)
		}
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

func (suite *UnitTestSuite) TestThresholdLogger() {
	suite.Run("Legacy semantic conventions", func() {
		suite.runThresholdLoggerTest(true, false)
	})

	suite.Run("Stable semantic conventions", func() {
		suite.runThresholdLoggerTest(false, true)
	})

	suite.Run("Both legacy and stable semantic conventions", func() {
		suite.runThresholdLoggerTest(true, true)
	})
}
