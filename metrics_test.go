package gocb

import (
	"sync"
	"sync/atomic"
)

type testCounter struct {
	count uint64
}

func (tc *testCounter) IncrementBy(val uint64) {
	atomic.AddUint64(&tc.count, val)
}

type testValueRecorderEntry struct {
	value uint64
	tags  map[string]string
}

type testValueRecorderEntries struct {
	entries []testValueRecorderEntry
	lock    sync.Mutex
}

func (e *testValueRecorderEntries) AddEntry(val uint64, tags map[string]string) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.entries = append(e.entries, testValueRecorderEntry{
		value: val,
		tags:  tags,
	})
}

type testValueRecorder struct {
	entries *testValueRecorderEntries
	tags    map[string]string
}

func (tvr *testValueRecorder) RecordValue(val uint64) {
	tvr.entries.AddEntry(val, tvr.tags)
}

type testMeterKey struct {
	metricName string
	service    string
	operation  string
}

type testMeter struct {
	lock     sync.Mutex
	counters map[testMeterKey]*testCounter
	values   map[testMeterKey]*testValueRecorderEntries
}

func newTestMeter() *testMeter {
	return &testMeter{
		counters: make(map[testMeterKey]*testCounter),
		values:   make(map[testMeterKey]*testValueRecorderEntries),
	}
}

func (tm *testMeter) Reset() {
	tm.lock.Lock()
	tm.counters = make(map[testMeterKey]*testCounter)
	tm.values = make(map[testMeterKey]*testValueRecorderEntries)
	tm.lock.Unlock()
}

func (tm *testMeter) Counter(name string, tags map[string]string) (Counter, error) {
	key := makeMetricsKeyFromTags(name, tags)

	tm.lock.Lock()
	counter := tm.counters[key]
	if counter == nil {
		counter = &testCounter{}
		tm.counters[key] = counter
	}
	tm.lock.Unlock()

	return counter, nil
}

func (tm *testMeter) ValueRecorder(name string, tags map[string]string) (ValueRecorder, error) {
	key := makeMetricsKeyFromTags(name, tags)

	tm.lock.Lock()
	recorderEntries := tm.values[key]
	if recorderEntries == nil {
		recorderEntries = &testValueRecorderEntries{}
		tm.values[key] = recorderEntries
	}
	tm.lock.Unlock()

	return &testValueRecorder{
		entries: recorderEntries,
		tags:    tags,
	}, nil
}

func (suite *IntegrationTestSuite) AssertKVMetrics(metricName, op string, length int, atLeastLen bool) {
	suite.AssertMetrics(makeMetricsKey(metricName, "kv", op), length, atLeastLen)
}

func makeMetricsKey(metricName, service, op string) testMeterKey {
	return testMeterKey{
		metricName: metricName,
		service:    service,
		operation:  op,
	}
}

func makeMetricsKeyFromTags(metricName string, tags map[string]string) testMeterKey {
	key := testMeterKey{
		metricName: metricName,
		service:    tags[meterLegacyAttribService],
		operation:  tags[meterLegacyAttribOperationName],
	}
	if key.service == "" {
		key.service = tags[meterStableAttribService]
	}
	if key.operation == "" {
		key.operation = tags[meterStableAttribOperationName]
	}
	return key
}

func (suite *IntegrationTestSuite) AssertMetrics(key testMeterKey, length int, atLeastLen bool) {
	globalMeter.lock.Lock()
	defer globalMeter.lock.Unlock()
	values := globalMeter.values
	if suite.Assert().Contains(values, key) {
		if atLeastLen {
			suite.Assert().GreaterOrEqual(len(values[key].entries), length)
		} else {
			suite.Assert().Len(values[key].entries, length)
		}
		for _, entry := range values[key].entries {
			suite.Assert().NotZero(entry.value)
		}
	}
}

func (suite *UnitTestSuite) runMetricsConventionsTest(conventions []ObservabilitySemanticConvention, expectingLegacy, expectingStable bool) {
	meter := newMeterWrapper(newTestMeter(), ObservabilityConfig{
		SemanticConventionOptIn: conventions,
	})
	meter.clusterLabelsProvider = unitTestClusterLabelsProvider{}

	meter.valueRecordWithDuration(
		"kv",
		"get",
		750,
		&keyspace{
			bucketName:     "test-bucket",
			scopeName:      "test-scope",
			collectionName: "test-collection",
		},
		ErrDocumentNotFound,
	)

	meter.valueRecordWithDuration(
		"kv",
		"get",
		750,
		&keyspace{
			bucketName:     "test-bucket",
			scopeName:      "test-scope",
			collectionName: "test-collection",
		},
		nil,
	)

	legacyMetrics := meter.meter.(*testMeter).values[makeMetricsKey(meterNameCBOperations, "kv", "get")]
	stableMetrics := meter.meter.(*testMeter).values[makeMetricsKey(meterNameDBClientOperationDuration, "kv", "get")]

	if expectingLegacy {
		suite.Assert().Len(legacyMetrics.entries, 2)

		for idx, entry := range legacyMetrics.entries {
			suite.Assert().Equal(uint64(750), entry.value)
			suite.Assert().Equal("kv", entry.tags[meterLegacyAttribService])
			suite.Assert().Equal("get", entry.tags[meterLegacyAttribOperationName])
			suite.Assert().Equal("test-bucket", entry.tags[meterLegacyAttribBucketName])
			suite.Assert().Equal("test-scope", entry.tags[meterLegacyAttribScopeName])
			suite.Assert().Equal("test-collection", entry.tags[meterLegacyAttribCollectionName])
			suite.Assert().Equal("test-cluster-uuid", entry.tags[meterLegacyAttribClusterUUID])
			suite.Assert().Equal("test-cluster-name", entry.tags[meterLegacyAttribClusterName])
			if idx == 0 {
				suite.Assert().Equal("DocumentNotFound", entry.tags[meterLegacyAttribOutcome])
			} else {
				suite.Assert().Equal("Success", entry.tags[meterLegacyAttribOutcome])
			}
			suite.Assert().Len(entry.tags, 8)
		}
	} else {
		suite.Assert().Nil(legacyMetrics)
	}

	if expectingStable {
		suite.Assert().Len(stableMetrics.entries, 2)

		for idx, entry := range stableMetrics.entries {
			suite.Assert().Equal(uint64(750), entry.value)
			suite.Assert().Equal("couchbase", entry.tags[meterStableAttribSystemName])
			suite.Assert().Equal("kv", entry.tags[meterStableAttribService])
			suite.Assert().Equal("get", entry.tags[meterStableAttribOperationName])
			suite.Assert().Equal("test-bucket", entry.tags[meterStableAttribBucketName])
			suite.Assert().Equal("test-scope", entry.tags[meterStableAttribScopeName])
			suite.Assert().Equal("test-collection", entry.tags[meterStableAttribCollectionName])
			suite.Assert().Equal("test-cluster-uuid", entry.tags[meterStableAttribClusterUUID])
			suite.Assert().Equal("test-cluster-name", entry.tags[meterStableAttribClusterName])
			suite.Assert().Equal("s", entry.tags[meterReservedAttribUnit])
			if idx == 0 {
				suite.Assert().Equal("DocumentNotFound", entry.tags[meterStableAttribErrorType])
				suite.Assert().Len(entry.tags, 10)
			} else {
				suite.Assert().NotContains(entry.tags, meterStableAttribErrorType)
				suite.Assert().Len(entry.tags, 9)
			}
		}
	} else {
		suite.Assert().Nil(stableMetrics)
	}
}

func (suite *UnitTestSuite) TestMetricsConventions() {
	suite.Run("Default", func() {
		suite.runMetricsConventionsTest(nil, true, false)
	})
	suite.Run("Database", func() {
		suite.runMetricsConventionsTest([]ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabase,
		}, false, true)
	})
	suite.Run("DatabaseDup", func() {
		suite.runMetricsConventionsTest([]ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabaseDup,
		}, true, true)
	})
	suite.Run("DatabaseDup & Database", func() {
		suite.runMetricsConventionsTest([]ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabase,
			ObservabilitySemanticConventionDatabaseDup,
		}, true, true)
	})
}
