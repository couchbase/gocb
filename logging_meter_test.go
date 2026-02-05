package gocb

import (
	"time"
)

func (suite *UnitTestSuite) TestLatencyHistogram() {
	histo := newLatencyHistogram(2000000, 1000, 1.5)
	suite.Require().Len(histo.bins, 21)

	histo.RecordValue(1000)
	histo.RecordValue(1000)
	histo.RecordValue(1000)
	histo.RecordValue(1000)
	histo.RecordValue(1000)
	histo.RecordValue(100000)
	histo.RecordValue(200000)
	histo.RecordValue(300000)
	histo.RecordValue(500000)
	histo.RecordValue(2000000)

	chisto := histo.AggregateAndReset()
	suite.Assert().Equal("<= 1000.00", chisto.BinAtPercentile(50))
	suite.Assert().Equal("<= 129746.34", chisto.BinAtPercentile(60))
	suite.Assert().Equal("<= 291929.26", chisto.BinAtPercentile(70))
	suite.Assert().Equal("<= 437893.89", chisto.BinAtPercentile(80))
	suite.Assert().Equal("<= 656840.84", chisto.BinAtPercentile(90))
	suite.Assert().Equal("<= 2216837.82", chisto.BinAtPercentile(100))
}

func (suite *UnitTestSuite) TestLatencyHistogramGreaterThanMax() {
	histo := newLatencyHistogram(2000000, 1000, 1.5)
	histo.RecordValue(4000000)

	chisto := histo.AggregateAndReset()
	suite.Assert().Equal("> 2216837.82", chisto.BinAtPercentile(100))
}

func (suite *UnitTestSuite) runWrappedLoggingMeterTest(conventions []ObservabilitySemanticConvention) {
	meter := newAggregatingMeter(&LoggingMeterOptions{
		EmitInterval: 10 * time.Second,
	})
	mw := newMeterWrapper(meter, ObservabilityConfig{
		SemanticConventionOptIn: conventions,
	})
	for _, val := range []uint64{1000, 1000, 10000, 20000, 1500} {
		mw.valueRecordWithDuration("kv", "get", val, nil, nil)
	}
	for _, val := range []uint64{2000, 1000, 3500, 10000, 20000, 50000} {
		mw.valueRecordWithDuration("kv", "replace", val, nil, nil)
	}
	mw.valueRecordWithDuration("query", "query", 112000, nil, nil)

	// The output must always be the same irrespective of the conventions used
	output := meter.generateOutput()
	meta := output["meta"].(map[string]interface{})
	suite.Assert().Equal(10, meta["emit_interval_s"])

	suite.Require().Contains(output, "kv")
	kvOutput := output["kv"].(map[string]interface{})

	suite.Require().Contains(output, "query")
	queryOutput := output["query"].(map[string]interface{})

	suite.Require().Contains(kvOutput, "get")
	suite.Require().Contains(queryOutput, "query")
	suite.Require().Contains(kvOutput, "replace")

	output1 := kvOutput["get"].(map[string]interface{})
	output2 := kvOutput["replace"].(map[string]interface{})
	qoutput := queryOutput["query"].(map[string]interface{})

	suite.Assert().Equal(uint64(5), output1["total_count"])
	suite.Assert().Equal(uint64(6), output2["total_count"])
	suite.Assert().Equal(uint64(1), qoutput["total_count"])

	percentiles1 := output1["percentiles_us"].(map[string]string)
	percentiles2 := output2["percentiles_us"].(map[string]string)
	percentilesq := qoutput["percentiles_us"].(map[string]string)

	suite.Assert().Equal("<= 1500.00", percentiles1["50.0"])
	suite.Assert().Equal("<= 25628.91", percentiles1["90.0"])
	suite.Assert().Equal("<= 25628.91", percentiles1["99.0"])
	suite.Assert().Equal("<= 25628.91", percentiles1["99.9"])
	suite.Assert().Equal("<= 25628.91", percentiles1["100.0"])

	suite.Assert().Equal("<= 5062.50", percentiles2["50.0"])
	suite.Assert().Equal("<= 57665.04", percentiles2["90.0"])
	suite.Assert().Equal("<= 57665.04", percentiles2["99.0"])
	suite.Assert().Equal("<= 57665.04", percentiles2["99.9"])
	suite.Assert().Equal("<= 57665.04", percentiles2["100.0"])

	suite.Assert().Equal("<= 129746.34", percentilesq["50.0"])
	suite.Assert().Equal("<= 129746.34", percentilesq["90.0"])
	suite.Assert().Equal("<= 129746.34", percentilesq["99.0"])
	suite.Assert().Equal("<= 129746.34", percentilesq["99.9"])
	suite.Assert().Equal("<= 129746.34", percentilesq["100.0"])
}

func (suite *UnitTestSuite) TestWrappedLoggingMeter() {
	suite.Run("Default", func() {
		suite.runWrappedLoggingMeterTest(nil)
	})
	suite.Run("Database", func() {
		suite.runWrappedLoggingMeterTest([]ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabase,
		})
	})
	suite.Run("DatabaseDup", func() {
		suite.runWrappedLoggingMeterTest([]ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabaseDup,
		})
	})
	suite.Run("DatabaseDup & Database", func() {
		suite.runWrappedLoggingMeterTest([]ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabase,
			ObservabilitySemanticConventionDatabaseDup,
		})
	})
}

func (suite *UnitTestSuite) TestLoggingMeter() {
	meter := newAggregatingMeter(&LoggingMeterOptions{
		EmitInterval: 10 * time.Second,
	})
	r1, err := meter.ValueRecorder(meterNameCBOperations, map[string]string{
		meterLegacyAttribService:       "kv",
		meterLegacyAttribOperationName: "get",
	})
	suite.Require().Nil(err)
	r2, err := meter.ValueRecorder(meterNameCBOperations, map[string]string{
		meterLegacyAttribService:       "kv",
		meterLegacyAttribOperationName: "replace",
	})
	suite.Require().Nil(err)
	r3, err := meter.ValueRecorder(meterNameCBOperations, map[string]string{
		meterLegacyAttribService:       "query",
		meterLegacyAttribOperationName: "query",
	})
	suite.Require().Nil(err)

	r1.RecordValue(1000)
	r1.RecordValue(1000)
	r1.RecordValue(10000)
	r1.RecordValue(20000)
	r1.RecordValue(1500)

	r2.RecordValue(2000)
	r2.RecordValue(1000)
	r2.RecordValue(3500)
	r2.RecordValue(10000)
	r2.RecordValue(20000)
	r2.RecordValue(50000)

	r3.RecordValue(112000)

	output := meter.generateOutput()
	meta := output["meta"].(map[string]interface{})
	suite.Assert().Equal(10, meta["emit_interval_s"])

	suite.Require().Contains(output, "kv")
	kvOutput := output["kv"].(map[string]interface{})

	suite.Require().Contains(output, "query")
	queryOutput := output["query"].(map[string]interface{})

	suite.Require().Contains(kvOutput, "get")
	suite.Require().Contains(queryOutput, "query")
	suite.Require().Contains(kvOutput, "replace")

	output1 := kvOutput["get"].(map[string]interface{})
	output2 := kvOutput["replace"].(map[string]interface{})
	qoutput := queryOutput["query"].(map[string]interface{})

	suite.Assert().Equal(uint64(5), output1["total_count"])
	suite.Assert().Equal(uint64(6), output2["total_count"])
	suite.Assert().Equal(uint64(1), qoutput["total_count"])

	percentiles1 := output1["percentiles_us"].(map[string]string)
	percentiles2 := output2["percentiles_us"].(map[string]string)
	percentilesq := qoutput["percentiles_us"].(map[string]string)

	suite.Assert().Equal("<= 1500.00", percentiles1["50.0"])
	suite.Assert().Equal("<= 25628.91", percentiles1["90.0"])
	suite.Assert().Equal("<= 25628.91", percentiles1["99.0"])
	suite.Assert().Equal("<= 25628.91", percentiles1["99.9"])
	suite.Assert().Equal("<= 25628.91", percentiles1["100.0"])

	suite.Assert().Equal("<= 5062.50", percentiles2["50.0"])
	suite.Assert().Equal("<= 57665.04", percentiles2["90.0"])
	suite.Assert().Equal("<= 57665.04", percentiles2["99.0"])
	suite.Assert().Equal("<= 57665.04", percentiles2["99.9"])
	suite.Assert().Equal("<= 57665.04", percentiles2["100.0"])

	suite.Assert().Equal("<= 129746.34", percentilesq["50.0"])
	suite.Assert().Equal("<= 129746.34", percentilesq["90.0"])
	suite.Assert().Equal("<= 129746.34", percentilesq["99.0"])
	suite.Assert().Equal("<= 129746.34", percentilesq["99.9"])
	suite.Assert().Equal("<= 129746.34", percentilesq["100.0"])
}
