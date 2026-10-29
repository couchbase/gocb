package gocb

func (suite *UnitTestSuite) TestPsOpManagerFinishWithoutRetryRequest() {
	tracer := newTestTracer()
	provider := newPsOpManagerProvider(nil, newTracerWrapper(tracer, ObservabilityConfig{}), 0, nil, "query")

	manager := provider.NewManager(nil, "query", psOpSpanAttributes{})

	suite.Require().NotPanics(manager.Finish)

	spans := tracer.GetSpans()[nil]
	suite.Require().Len(spans, 1)
	suite.Assert().True(spans[0].Finished)
	suite.Assert().Equal(uint32(0), spans[0].Tags[spanLegacyAttribNumRetries])
}

func (suite *UnitTestSuite) TestPsOpManagerFinishWithRetryRequest() {
	tracer := newTestTracer()
	provider := newPsOpManagerProvider(nil, newTracerWrapper(tracer, ObservabilityConfig{}), 0, nil, "query")

	manager := provider.NewManager(nil, "query", psOpSpanAttributes{})
	req := newRetriableRequestPS("query", false, nil, "", nil)
	req.recordRetryAttempt(ServiceNotAvailableRetryReason)
	req.recordRetryAttempt(ServiceNotAvailableRetryReason)
	manager.SetRetryRequest(req)

	manager.Finish()

	spans := tracer.GetSpans()[nil]
	suite.Require().Len(spans, 1)
	suite.Assert().True(spans[0].Finished)
	suite.Assert().Equal(uint32(2), spans[0].Tags[spanLegacyAttribNumRetries])
}
