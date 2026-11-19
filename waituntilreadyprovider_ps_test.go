package gocb

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type healthCheckResult struct {
	status grpc_health_v1.HealthCheckResponse_ServingStatus
	err    error
}

// stubHealthClient returns the configured results in order, repeating the last one once exhausted.
type stubHealthClient struct {
	grpc_health_v1.HealthClient
	results []healthCheckResult
	calls   int
}

func (c *stubHealthClient) Check(_ context.Context, req *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption,
) (*grpc_health_v1.HealthCheckResponse, error) {
	res := c.results[min(c.calls, len(c.results)-1)]
	c.calls++

	if req.GetService() != "" {
		return nil, errors.New("expected service to be unset")
	}
	if res.err != nil {
		return nil, res.err
	}

	return &grpc_health_v1.HealthCheckResponse{Status: res.status}, nil
}

func (suite *UnitTestSuite) newWaitUntilReadyProviderPs(client grpc_health_v1.HealthClient, tracer RequestTracer,
) *waitUntilReadyProviderPs {
	return &waitUntilReadyProviderPs{
		client: client,
		managerProvider: newPsOpManagerProvider(
			NewBestEffortRetryStrategy(nil),
			newTracerWrapper(tracer, ObservabilityConfig{}),
			0, nil, ""),
	}
}

func (suite *UnitTestSuite) TestWaitUntilReadyPsServing() {
	client := &stubHealthClient{
		results: []healthCheckResult{
			{status: grpc_health_v1.HealthCheckResponse_SERVING},
		},
	}
	tracer := newTestTracer()
	provider := suite.newWaitUntilReadyProviderPs(client, tracer)

	err := provider.WaitUntilReady(context.Background(), time.Now().Add(time.Second), &WaitUntilReadyOptions{
		DesiredState: ClusterStateOffline,
		ServiceTypes: []ServiceType{ServiceTypeQuery},
	})
	suite.Require().NoError(err)
	suite.Assert().Equal(1, client.calls)

	spans := tracer.GetSpans()[nil]
	suite.Require().Len(spans, 1)
	wurSpan := spans[0]
	suite.Assert().Equal("wait_until_ready", wurSpan.Name)
	suite.Assert().True(wurSpan.Finished)
	suite.Assert().NotContains(wurSpan.Tags, spanLegacyAttribService)

	healthSpans := wurSpan.Spans["health_check"]
	suite.Require().Len(healthSpans, 1)
	suite.Assert().True(healthSpans[0].Finished)
	suite.Assert().Empty(healthSpans[0].Spans)
}

func (suite *UnitTestSuite) TestWaitUntilReadyPsRetriesNotServing() {
	client := &stubHealthClient{
		results: []healthCheckResult{
			{status: grpc_health_v1.HealthCheckResponse_NOT_SERVING},
			{err: status.Error(codes.Unavailable, "connection refused")},
			{status: grpc_health_v1.HealthCheckResponse_SERVING},
		},
	}
	tracer := newTestTracer()
	provider := suite.newWaitUntilReadyProviderPs(client, tracer)

	err := provider.WaitUntilReady(context.Background(), time.Now().Add(5*time.Second), &WaitUntilReadyOptions{})
	suite.Require().NoError(err)
	suite.Assert().Equal(3, client.calls)

	wurSpan := tracer.GetSpans()[nil][0]
	suite.Assert().Len(wurSpan.Spans["health_check"], 3)
	suite.Assert().Equal(uint32(2), wurSpan.Tags[spanLegacyAttribNumRetries])
}

func (suite *UnitTestSuite) TestWaitUntilReadyPsTimeout() {
	client := &stubHealthClient{
		results: []healthCheckResult{
			{err: status.Error(codes.Unavailable, "connection refused")},
		},
	}
	provider := suite.newWaitUntilReadyProviderPs(client, newTestTracer())

	err := provider.WaitUntilReady(context.Background(), time.Now().Add(100*time.Millisecond), &WaitUntilReadyOptions{})
	suite.Require().ErrorIs(err, ErrUnambiguousTimeout)

	var tErr *TimeoutError
	suite.Require().ErrorAs(err, &tErr)
	suite.Assert().Equal([]RetryReason{ServiceNotAvailableRetryReason}, tErr.RetryReasons)
	suite.Assert().Greater(tErr.RetryAttempts, uint32(0))
}

func (suite *UnitTestSuite) TestWaitUntilReadyPsFastFailsNonRetriableError() {
	client := &stubHealthClient{
		results: []healthCheckResult{
			{err: status.Error(codes.Internal, "unknown failure")},
		},
	}
	provider := suite.newWaitUntilReadyProviderPs(client, newTestTracer())

	err := provider.WaitUntilReady(context.Background(), time.Now().Add(time.Second), &WaitUntilReadyOptions{})
	suite.Require().ErrorIs(err, ErrInternalServerFailure)
	suite.Assert().Equal(1, client.calls)
}

func (suite *UnitTestSuite) TestWaitUntilReadyPsUsesRetryStrategyOption() {
	client := &stubHealthClient{
		results: []healthCheckResult{
			{status: grpc_health_v1.HealthCheckResponse_NOT_SERVING},
		},
	}
	provider := suite.newWaitUntilReadyProviderPs(client, newTestTracer())

	err := provider.WaitUntilReady(context.Background(), time.Now().Add(time.Second), &WaitUntilReadyOptions{
		RetryStrategy: &failFastRetryStrategy{},
	})
	suite.Require().ErrorIs(err, ErrServiceNotAvailable)
	suite.Assert().Equal(1, client.calls)
}
