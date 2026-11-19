package gocb

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type waitUntilReadyProviderPs struct {
	client          grpc_health_v1.HealthClient
	managerProvider *psOpManagerProvider
}

// WaitUntilReady sends gRPC health check requests until one reports SERVING, retrying until the deadline.
// DesiredState and ServiceTypes are ignored, as the health check provides no per-service or cluster state granularity.
func (wpw *waitUntilReadyProviderPs) WaitUntilReady(ctx context.Context, deadline time.Time, opts *WaitUntilReadyOptions) error {
	manager := wpw.managerProvider.NewManager(nil, "wait_until_ready", psOpSpanAttributes{})
	defer manager.Finish()

	manager.SetContext(ctx)
	manager.SetIsIdempotent(true)
	manager.SetRetryStrategy(opts.RetryStrategy)

	ctx, cancel := context.WithDeadline(manager.Context(), deadline)
	defer cancel()

	sendHealthCheck := func(ctx context.Context, req *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption,
	) (*grpc_health_v1.HealthCheckResponse, error) {
		span := manager.Tracer().CreateSubOperationSpan(manager.TraceSpan(), "health_check", "")
		defer span.End()

		if s, ok := span.Wrapped().(OtelAwareRequestSpan); ok {
			ctx = trace.ContextWithSpan(ctx, s.Wrapped())
		}

		return wpw.client.Check(ctx, req, opts...)
	}

	checkServing := func(resp *grpc_health_v1.HealthCheckResponse) error {
		if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			return wrapError(ErrServiceNotAvailable, "health check returned status "+resp.GetStatus().String())
		}

		return nil
	}

	_, err := wrapPSOpCtxWithPeek(ctx, manager, &grpc_health_v1.HealthCheckRequest{}, manager.TraceSpan().Wrapped(),
		sendHealthCheck, checkServing)

	return err
}
