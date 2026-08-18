//go:build !sdk

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/cluster"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/counter"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/executor"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/getreplicas"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv"
	fitSearch "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/search"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"
	fitStreams "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/streams"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/telemetry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fitLookupIn "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/lookupin"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/lookupin"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/rangescan"
	rangescan2 "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/rangescan"

	gocbopentelemetry "github.com/couchbase/gocb-opentelemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"google.golang.org/grpc/credentials"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"

	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/transactions"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/transactions/hooks"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/observability"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/performer"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	protoSDK "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/streams"
	protoTransactions "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/transactions"
)

type Performer struct {
	conns            map[string]*cluster.Connection
	lock             sync.Mutex
	logger           *logrus.Logger
	performerVersion string
	streams          *fitStreams.StreamOwner
	counters         *counter.Counters

	spanOwner       *telemetry.SpanOwner
	telemetryLock   sync.Mutex
	tracerProviders map[string]*sdktrace.TracerProvider
	meterProviders  map[string]*metricsdk.MeterProvider

	protocol.UnimplementedPerformerServiceServer
}

func NewPerformer(logger *logrus.Logger, version string) *Performer {
	gocb.SetLogger(helpers.NewLogger(logger))
	gocb.SetLogRedactionLevel(gocb.RedactPartial)

	return &Performer{
		logger:           logger,
		performerVersion: version,
		conns:            make(map[string]*cluster.Connection),
		streams:          fitStreams.NewStreamOwner(logger),
		spanOwner:        telemetry.NewSpanOwner(),
		counters:         counter.NewCounters(),
		tracerProviders:  make(map[string]*sdktrace.TracerProvider),
		meterProviders:   make(map[string]*metricsdk.MeterProvider),
	}
}

func (p *Performer) getConnLocked(connID string) *cluster.Connection {
	if p.conns == nil {
		return nil
	}

	if conn, isValid := p.conns[connID]; isValid {
		return conn
	}

	return nil
}

func (p *Performer) PerformerCapsFetch(context.Context, *performer.PerformerCapsFetchRequest) (*performer.PerformerCapsFetchResponse, error) {
	p.logger.Log(logrus.InfoLevel, "PerformerCapsFetch called")

	var libCaps []protoTransactions.Caps
	libProtoVer := gocb.TransactionsProtocolVersion()
	libProtoExts := gocb.TransactionsProtocolExtensions()
	for _, capName := range libProtoExts {
		if capID, ok := protoTransactions.Caps_value[capName]; ok {
			libCaps = append(libCaps, protoTransactions.Caps(capID))
		} else {
			return nil, status.Errorf(codes.Aborted, "library reported unexpected feature %s:", capName)
		}
	}

	sdkCaps := []protoSDK.Caps{protoSDK.Caps_SDK_QUERY_INDEX_MANAGEMENT, protoSDK.Caps_SDK_LOOKUP_IN,
		protoSDK.Caps_SDK_QUERY, protoSDK.Caps_SDK_BUCKET_MANAGEMENT, protoSDK.Caps_SDK_COLLECTION_MANAGEMENT,
		protoSDK.Caps_SDK_KV, protoSDK.Caps_SDK_SEARCH, protoSDK.Caps_SDK_SEARCH_INDEX_MANAGEMENT, protoSDK.Caps_WAIT_UNTIL_READY,
		protoSDK.Caps_SUPPORTS_AUTHENTICATOR}
	performerCaps := []performer.Caps{performer.Caps_GRPC_TESTING, performer.Caps_KV_SUPPORT_1,
		performer.Caps_CLUSTER_CONFIG_1, performer.Caps_CLUSTER_CONFIG_CERT, performer.Caps_OBSERVABILITY_1,
		performer.Caps_CONTENT_AS_PERFORMER_VALIDATION,
	}

	performerCaps = append(performerCaps, performer.Caps_TRANSACTIONS_SUPPORT_1, performer.Caps_TRANSACTIONS_WORKLOAD_1, performer.Caps_TXN_CLIENT_CONTEXT_ID_SUPPORT)

	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_COLLECTION_QUERY_INDEX_MANAGEMENT)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_KV_RANGE_SCAN, protoSDK.Caps_SDK_KV_RANGE_SCAN, protoSDK.Caps_SDK_LOOKUP_IN_REPLICAS, protoSDK.Caps_SDK_QUERY_READ_FROM_REPLICA,
		protoSDK.Caps_SDK_MANAGEMENT_HISTORY_RETENTION)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_DOCUMENT_NOT_LOCKED, protoSDK.Caps_SDK_QUERY_BOTH_POSITIONAL_AND_NAMED_PARAMETERS)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_VECTOR_SEARCH, protoSDK.Caps_SDK_SCOPE_SEARCH, protoSDK.Caps_SDK_SCOPE_SEARCH_INDEX_MANAGEMENT)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_SEARCH_RFC_REVISION_11)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_INDEX_MANAGEMENT_RFC_REVISION_25)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_VECTOR_SEARCH_BASE64)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_ZONE_AWARE_READ_FROM_REPLICA)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_OBSERVABILITY_CLUSTER_LABELS)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_OBSERVABILITY_RFC_REV_24)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_APP_TELEMETRY, protoSDK.Caps_SDK_BUCKET_SETTINGS_NUM_VBUCKETS)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_PREFILTER_VECTOR_SEARCH)
	sdkCaps = append(sdkCaps,
		protoSDK.Caps_SDK_SET_AUTHENTICATOR,
		protoSDK.Caps_SDK_JWT,
		protoSDK.Caps_SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS,
	)
	sdkCaps = append(sdkCaps, protoSDK.Caps_SDK_QUERY_2120)

	return &performer.PerformerCapsFetchResponse{
		TransactionImplementationsCaps: libCaps,
		PerformerUserAgent:             "go",
		PerformerCaps:                  performerCaps,
		LibraryVersion:                 gocb.Version()[1:],
		TransactionsProtocolVersion:    &libProtoVer,
		SupportedApis:                  []shared.API{shared.API_DEFAULT},
		SdkImplementationCaps:          sdkCaps,
	}, nil
}

func (p *Performer) ClusterConnectionCreate(ctx context.Context, req *shared.ClusterConnectionCreateRequest) (*shared.ClusterConnectionCreateResponse, error) {
	p.logger.Logf(logrus.InfoLevel, "ClusterConnectionCreate called with ID=%s", req.ClusterConnectionId)

	var auth gocb.Authenticator
	if req.Authenticator == nil {
		auth = &gocb.PasswordAuthenticator{
			Username: req.ClusterUsername,
			Password: req.ClusterPassword,
		}
	} else {
		switch a := req.Authenticator.Authenticator.(type) {
		case nil:
			auth = &gocb.PasswordAuthenticator{
				Username: req.ClusterUsername,
				Password: req.ClusterPassword,
			}
		case *shared.Authenticator_PasswordAuth:
			auth = &gocb.PasswordAuthenticator{
				Username: a.PasswordAuth.Username,
				Password: a.PasswordAuth.Password,
			}
		case *shared.Authenticator_CertificateAuth:
			cert, err := tls.X509KeyPair([]byte(a.CertificateAuth.Cert), []byte(a.CertificateAuth.Key))
			if err != nil {
				p.logger.Warnf("Error reading client cert: %v", err)
				return nil, status.Errorf(codes.Aborted, "unexpected error reading client cert: %v", err)
			}

			auth = &gocb.CertificateAuthenticator{
				ClientCertificate: &cert,
			}

		case *shared.Authenticator_JwtAuth:
			auth = &gocb.JWTAuthenticator{
				Token: a.JwtAuth.Jwt,
			}

		}
	}

	hostname, cfg, err := p.protoToGocbConnectOpts(ctx, req.ClusterHostname, auth, req.ClusterConfig, req.ClusterConnectionId)
	if err != nil {
		p.logger.Warnf("Error converting cluster options: %v", err)
		return nil, status.Errorf(codes.Aborted, "unexpected error converting cluster options: %v", err)
	}

	tHooks := hooks.NewTransactionHooks()
	cHooks := hooks.NewCleanupHooks()
	crHooks := hooks.NewClientRecordHooks()
	cfg.TransactionsConfig.Internal.Hooks = tHooks
	cfg.TransactionsConfig.Internal.CleanupHooks = cHooks
	cfg.TransactionsConfig.Internal.ClientRecordHooks = crHooks

	conn, err := cluster.Connect(hostname, cfg)
	if err != nil {
		p.logger.Warnf("Error connecting to cluster: %v", err)
		return nil, status.Errorf(codes.Aborted, "cannot create connection: %v", err)
	}

	// This feels nasty, probably a better way to do it.
	if req.ClusterConfig != nil && req.ClusterConfig.TransactionsConfig != nil {
		if err := tHooks.Configure(conn, req.ClusterConfig.TransactionsConfig.Hook); err != nil {
			p.logger.Warnf("Error configuring transactions hook: %v", err)
			return nil, status.Errorf(codes.Aborted, "cannot configure transactions hook: %v", err)
		}
		if err := cHooks.Configure(conn, req.ClusterConfig.TransactionsConfig.Hook); err != nil {
			p.logger.Warnf("Error configuring cleanup hook: %v", err)
			return nil, status.Errorf(codes.Aborted, "cannot configure cleanup hook: %v", err)
		}
		if err := crHooks.Configure(conn, req.ClusterConfig.TransactionsConfig.Hook); err != nil {
			p.logger.Warnf("Error configuring client record hook: %v", err)
			return nil, status.Errorf(codes.Aborted, "cannot configure client record hook: %v", err)
		}
	}

	p.lock.Lock()
	p.conns[req.ClusterConnectionId] = conn
	numConns := len(p.conns)
	p.lock.Unlock()

	return &shared.ClusterConnectionCreateResponse{
		ClusterConnectionCount: int32(numConns),
	}, nil
}

func (p *Performer) ClusterConnectionClose(ctx context.Context, req *shared.ClusterConnectionCloseRequest) (*shared.ClusterConnectionCloseResponse, error) {
	p.logger.Logf(logrus.InfoLevel, "ClusterConnectionClose called with ID=%s", req.ClusterConnectionId)

	p.lock.Lock()
	conn := p.getConnLocked(req.ClusterConnectionId)
	if conn == nil {
		p.lock.Unlock()
		return nil, status.Errorf(codes.Unknown, "connection id %s not known", req.ClusterConnectionId)
	}
	delete(p.conns, req.ClusterConnectionId)
	p.lock.Unlock()

	var observabilityErrors []error

	p.telemetryLock.Lock()
	if tracer, ok := p.tracerProviders[req.ClusterConnectionId]; ok {
		err := tracer.ForceFlush(ctx)
		if err != nil {
			observabilityErrors = append(observabilityErrors, err)
		}
		err = tracer.Shutdown(ctx)
		if err != nil {
			observabilityErrors = append(observabilityErrors, err)
		}
		delete(p.tracerProviders, req.ClusterConnectionId)
	}
	if meter, ok := p.meterProviders[req.ClusterConnectionId]; ok {
		err := meter.ForceFlush(ctx)
		if err != nil {
			observabilityErrors = append(observabilityErrors, err)
		}
		err = meter.Shutdown(ctx)
		if err != nil {
			observabilityErrors = append(observabilityErrors, err)
		}
		delete(p.meterProviders, req.ClusterConnectionId)
	}
	p.telemetryLock.Unlock()

	if err := conn.Disconnect(); err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to disconnect connection %s: %v", req.ClusterConnectionId, err)
	}

	if len(observabilityErrors) > 0 {
		return &shared.ClusterConnectionCloseResponse{}, status.Errorf(codes.Aborted, "Failed to close observability providers for connection %s: %v", req.ClusterConnectionId, errors.Join(observabilityErrors...))
	}

	return &shared.ClusterConnectionCloseResponse{}, nil
}

// Disconnects any hanging connections to CB server. This ensures there is no disturbance from unnecessary connections to server
func (p *Performer) DisconnectConnections(ctx context.Context,
	in *shared.DisconnectConnectionsRequest) (*shared.DisconnectConnectionsResponse, error) {
	p.logger.Log(logrus.InfoLevel, "DisconnectConnections called")

	p.lock.Lock()
	conns := p.conns
	p.conns = make(map[string]*cluster.Connection)
	p.lock.Unlock()

	for connID, conn := range conns {
		err := conn.Disconnect()
		if err != nil {
			// Incase closing of any connection fails, just log it and proceed with closing other connections
			p.logger.Logf(
				logrus.InfoLevel,
				"DisconnectConnections failed to close connection for %s: %v",
				connID,
				err,
			)
		}
	}

	return &shared.DisconnectConnectionsResponse{}, nil
}

func (p *Performer) Run(request *run.Request, server protocol.PerformerService_RunServer) error {
	p.logger.Log(logrus.InfoLevel, "Run called")

	workloads, ok := request.Request.(*run.Request_Workloads)
	if !ok {
		return status.Errorf(codes.Aborted, "request not workloads")
	}

	p.lock.Lock()
	conn, ok := p.conns[workloads.Workloads.ClusterConnectionId]
	p.lock.Unlock()
	if !ok {
		return status.Errorf(codes.InvalidArgument, "connection not known for %s", workloads.Workloads.ClusterConnectionId)
	}

	var batchSize int32
	if request.Config != nil && request.Config.StreamingConfig != nil {
		batchSize = request.Config.StreamingConfig.GetBatchSize()
	}

	onError := make(chan error)
	doneCh := make(chan struct{})
	go func() {
		select {
		case <-onError:
			return
		case <-doneCh:
			return
		}
	}()

	runID := uuid.NewString()

	if request.Config != nil && request.Config.StreamingConfig.GetEnableMetrics() {
		metricsReporter, metricsErr := NewMetricsReporter(runID, server, p.logger)
		if metricsErr != nil {
			p.logger.Errorf("Failed to create metrics reporter: %v", metricsErr)
		} else {
			metricsReporter.Start()
			defer metricsReporter.Stop()
		}
	}

	// executor handles actually performing the operations required.
	executor := executor.NewExecutor(conn, p.counters, runID, p.streams, p.logger, p.spanOwner)
	transactionsExecutor := transactions.NewExecutor(conn, p.counters, p.logger)
	// The batcher handles streaming run results back to the driver, doing so in batches of a size
	// defined by the driver.
	batchHandler := NewBatcher(server, p.logger, onError, batchSize)

	// Start the batcher loop of checking if there are any results to write, and writing them if so.
	batchHandler.Run()

	// Create a runner per workload, the runner handles running the workloads - using the executor
	// to perform operations and then the batcher to send those results to the driver.
	var scaleRunners []*HorizontalScaleRunner
	for i, scaling := range workloads.Workloads.HorizontalScaling {
		r := NewHorizontalScaleRunner(p.logger, executor, p.counters, PerHorizontalRunner{
			Sender:      batchHandler,
			RunnerIndex: i,
			Workloads:   scaling.Workloads,
		})

		r.SetTransactionExecutor(transactionsExecutor)

		scaleRunners = append(scaleRunners, r)
	}

	// Run all of the workloads.
	for _, r := range scaleRunners {
		go r.Run()
	}

	p.logger.Logf(logrus.InfoLevel, "Started %d runners", len(scaleRunners))

	// Wait for all of the workloads to run to completion. If performer errors occur in any of the runners, the first one will be returned.
	var runnerErr error
	for _, r := range scaleRunners {
		r.Wait()
		if runnerErr == nil {
			runnerErr = r.Err()
		}
	}

	p.logger.Logf(logrus.InfoLevel, "All %d runners completed", len(scaleRunners))

	// If there are no streams for this run then this won't do much.
	p.streams.WaitForCompletion(runID)

	p.logger.Logf(logrus.InfoLevel, "All streams completed")

	// There's no more work to do so stop the batcher.
	<-batchHandler.Stop()

	// Shut down our error monitoring goroutine.
	close(doneCh)

	if prov, ok := p.tracerProviders[workloads.Workloads.ClusterConnectionId]; ok {
		p.logger.Logf(logrus.InfoLevel, "Flushing tracer provider")
		err := prov.ForceFlush(context.Background())
		if err != nil {
			p.logger.Logf(logrus.InfoLevel, "Failed to flush tracer: %s", err)
		}
	}

	if prov, ok := p.meterProviders[workloads.Workloads.ClusterConnectionId]; ok {
		p.logger.Logf(logrus.InfoLevel, "Flushing meter provider")
		err := prov.ForceFlush(context.Background())
		if err != nil {
			p.logger.Logf(logrus.InfoLevel, "Failed to flush meter: %s", err)
		}
	}

	return runnerErr
}

func (p *Performer) StreamCancel(ctx context.Context, request *streams.CancelRequest) (*streams.CancelResponse, error) {
	p.logger.Logf(logrus.InfoLevel, "StreamCancel called for %s", request.StreamId)
	stream := p.streams.Get(request.StreamId)
	if stream == nil {
		return nil, status.Errorf(codes.Unknown, "stream id %s not known", request.StreamId)
	}

	err := stream.Stream.Cancel()
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "failed to cancel stream")
	}

	stream.Sender.Send(&run.Result{
		Result: &run.Result_Stream{
			Stream: &streams.Signal{
				Signal: &streams.Signal_Cancelled{
					Cancelled: &streams.Cancelled{
						StreamId: request.StreamId,
					},
				},
			},
		},
	})

	stream.Stream.Finish()

	return &streams.CancelResponse{}, nil
}

func (p *Performer) StreamRequestItems(ctx context.Context, request *streams.RequestItemsRequest) (*streams.RequestItemsResponse, error) {
	p.logger.Logf(logrus.InfoLevel, "StreamRequestItems called for %s, requesting %d items", request.StreamId, request.NumItems)
	stream := p.streams.Get(request.StreamId)
	if stream == nil {
		return nil, status.Errorf(codes.Unknown, "stream id %s not known", request.StreamId)
	}

	switch scan := stream.Stream.(type) {
	default:
		return nil, status.Errorf(codes.Internal, "unknown stream type: %s", scan)

	case *rangescan2.RangeScanStream:
		err := p.rangeScanRequestItems(request, scan, stream.Sender)
		if err != nil {
			return nil, err
		}

		return &streams.RequestItemsResponse{}, nil
	case *fitLookupIn.LookupInAllReplicasStream:
		err := p.lookupInAllReplicasRequestItems(request, scan, stream.Sender, request.StreamId)
		if err != nil {
			return nil, err
		}

		return &streams.RequestItemsResponse{}, nil

	case *getreplicas.GetAllReplicasStream:
		err := p.getAllReplicasItems(request, scan, stream.Sender)
		if err != nil {
			return nil, err
		}

		return &streams.RequestItemsResponse{}, nil
	case *fitSearch.SearchStream:
		err := p.searchRequestItems(request, scan, stream.Sender, request.StreamId)
		if err != nil {
			return nil, err
		}

		return &streams.RequestItemsResponse{}, nil
	}
}

func (p *Performer) SpanCreate(ctx context.Context, req *observability.SpanCreateRequest) (*observability.SpanCreateResponse, error) {
	p.lock.Lock()
	conn := p.getConnLocked(req.ClusterConnectionId)
	p.lock.Unlock()
	if conn == nil {
		return nil, status.Errorf(codes.Unknown, "connection id %s not known", req.ClusterConnectionId)
	}

	var tracectx gocb.RequestSpanContext
	if req.ParentSpanId != nil {
		parent, ok := p.spanOwner.GetSpan(*req.ParentSpanId)
		if !ok {
			return nil, status.Errorf(codes.Internal, "unknown parent span id: %s", *req.ParentSpanId)
		}

		tracectx = parent.Context()
	}

	span := conn.Tracer().RequestSpan(tracectx, req.Name)
	for k, attrib := range req.Attributes {
		switch r := attrib.Value.(type) {
		case *observability.Attribute_ValueBoolean:
			span.SetAttribute(k, r.ValueBoolean)
		case *observability.Attribute_ValueString:
			span.SetAttribute(k, r.ValueString)
		case *observability.Attribute_ValueLong:
			span.SetAttribute(k, r.ValueLong)
		}
	}

	p.spanOwner.StoreSpan(req.Id, span)

	return &observability.SpanCreateResponse{}, nil
}

func (p *Performer) SpanFinish(ctx context.Context, req *observability.SpanFinishRequest) (*observability.SpanFinishResponse, error) {
	span, ok := p.spanOwner.GetSpan(req.Id)
	if !ok {
		p.lock.Unlock()
		return nil, status.Errorf(codes.Unknown, "span id %s not known", req.Id)
	}

	span.End()

	return &observability.SpanFinishResponse{}, nil
}

func (p *Performer) SetCounter(ctx context.Context, req *shared.Counter) (*shared.SetCounterResponse, error) {
	c, err := p.counters.Get(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to set counter with id %s", req.GetCounterId())
	}

	newValue := req.GetGlobal().GetCount()
	c.Set(newValue)

	p.logger.Infof("Set counter with id %s to %d", req.GetCounterId(), newValue)

	return &shared.SetCounterResponse{}, nil
}

func (p *Performer) ClearAllCounters(ctx context.Context, req *shared.ClearAllCountersRequest) (*shared.ClearAllCountersResponse, error) {
	p.counters.Clear()

	p.logger.Info("Cleared all counters")

	return &shared.ClearAllCountersResponse{}, nil
}

func (p *Performer) searchRequestItems(request *streams.RequestItemsRequest,
	stream *fitSearch.SearchStream, sender sender.ResultSender, streamID string) error {
	var sent int
	for i := 0; i < int(request.NumItems); i++ {
		streamItem := stream.Next()
		if streamItem == nil {
			meta, err := stream.Metadata()
			if err != nil {
				p.logger.Logf(logrus.InfoLevel, "Error detected on metadata call for %s: %s", streamID, err)
				sender.Send(helpers.MakeStreamErrorResult(streamID, err))
				return nil
			}
			facets, err := stream.Facets()
			if err != nil {
				p.logger.Logf(logrus.InfoLevel, "Error detected on facets call for %s: %s", streamID, err)
				sender.Send(helpers.MakeStreamErrorResult(streamID, err))
				return nil
			}

			sender.Send(helpers.MakeSearchStreamMetaResult(streamID, meta))
			sender.Send(helpers.MakeSearchStreamFacetsResult(streamID, facets))

			p.logger.Logf(logrus.InfoLevel, "No item returned on stream %s, have sent %d items", streamID, sent)
			if err := stream.Err(); err == nil {
				sender.Send(helpers.MakeStreamCompleteResult(streamID))
			} else {
				p.logger.Logf(logrus.InfoLevel, "Error detected on stream end for %s: %s", streamID, err)
				sender.Send(helpers.MakeStreamErrorResult(streamID, err))
			}
			return nil
		}
		sent++

		item := streamItem.(*fitSearch.SearchStreamItem) //nolint:errcheck

		row, err := item.Row()
		if err != nil {
			p.logger.Logf(logrus.InfoLevel, "Failed to parse row for stream %s: %s", streamID, err)
			sender.Send(helpers.MakeStreamErrorResult(streamID, err))
			continue
		}

		sender.Send(helpers.MakeSearchStreamRowResult(streamID, row))
	}

	p.logger.Logf(logrus.InfoLevel, "Sent %d items for %s", sent, request.StreamId)

	return nil
}

func (p *Performer) lookupInAllReplicasRequestItems(request *streams.RequestItemsRequest,
	stream *fitLookupIn.LookupInAllReplicasStream, sender sender.ResultSender, streamID string) error {
	var sent int
	for i := 0; i < int(request.NumItems); i++ {
		next := stream.Next()
		if next == nil {
			p.logger.Logf(logrus.InfoLevel, "No item returned on stream %s, have sent %d items", streamID, sent)
			sender.Send(helpers.MakeStreamCompleteResult(streamID))
			return nil
		}
		sent++

		item := fitLookupIn.ParseLookupInAllItem(next)

		sender.Send(helpers.MakeLookupInAllReplicasSuccessResult(&lookupin.LookupInAllReplicasResult{
			LookupInReplicaResult: item,
			StreamId:              streamID,
		}))
	}

	p.logger.Logf(logrus.InfoLevel, "Sent %d items for %s", sent, request.StreamId)

	return nil
}

func (p *Performer) rangeScanRequestItems(request *streams.RequestItemsRequest, stream *rangescan2.RangeScanStream, sender sender.ResultSender) error {
	var sent int
	for i := 0; i < int(request.NumItems); i++ {
		streamItem := stream.Next()
		if streamItem == nil {
			p.logger.Logf(logrus.InfoLevel, "No item returned on stream %s, have sent %d items", request.StreamId, sent)
			if err := stream.Err(); err == nil {
				sender.Send(helpers.MakeStreamCompleteResult(request.StreamId))
			} else {
				p.logger.Logf(logrus.InfoLevel, "Error detected on stream end for %s: %s", request.StreamId, err)
				sender.Send(helpers.MakeStreamErrorResult(request.StreamId, err))
			}
			return nil
		}
		item := streamItem.(*rangescan2.RangeScanResultItem) //nolint:errcheck
		sent++

		content, err := item.Content()
		if err != nil {
			p.logger.Logf(logrus.InfoLevel, "Failed to parse content for stream %s: %s", request.StreamId, err)
			sender.Send(helpers.MakeStreamErrorResult(request.StreamId, err))
			continue
		}
		result := &rangescan.ScanResult{
			Id:         item.ID(),
			StreamId:   request.StreamId,
			Cas:        item.Cas(),
			ExpiryTime: item.ExpiryTime(),
			Content:    content,
			IdOnly:     item.IDOnly(),
		}

		sender.Send(helpers.MakeRangeScanSuccessResult(result))
	}

	p.logger.Logf(logrus.InfoLevel, "Sent %d items for %s", sent, request.StreamId)

	return nil
}

func (p *Performer) getAllReplicasItems(request *streams.RequestItemsRequest, stream *getreplicas.GetAllReplicasStream, sender sender.ResultSender) error {
	var sent int
	for i := 0; i < int(request.NumItems); i++ {
		streamItem := stream.Next()
		if streamItem == nil {
			p.logger.Logf(logrus.InfoLevel, "No item returned on stream %s, have sent %d items", request.StreamId, sent)
			if err := stream.Err(); err == nil {
				sender.Send(helpers.MakeStreamCompleteResult(request.StreamId))
			} else {
				p.logger.Logf(logrus.InfoLevel, "Error detected on stream end for %s: %s", request.StreamId, err)
				sender.Send(helpers.MakeStreamErrorResult(request.StreamId, err))
			}
			return nil
		}
		item := streamItem.(*getreplicas.GetAllReplicasResultItem) //nolint:errcheck
		sent++

		content, err := helpers.ParseContentAs(item.ContentAs(), func(content interface{}) error {
			getResult := item.GetResult()
			return getResult.Content(&content)
		})
		if err != nil {
			p.logger.Logf(logrus.InfoLevel, "Failed to parse content for stream %s: %s", request.StreamId, err)
			sender.Send(helpers.MakeStreamErrorResult(request.StreamId, err))
			continue
		}
		result := &kv.GetReplicaResult{
			IsReplica: item.IsReplica(),
			Cas:       *item.Cas(),
			Content:   content,
		}

		sender.Send(helpers.MakeGetAllReplicasSuccessResult(result))
	}

	p.logger.Logf(logrus.InfoLevel, "Sent %d items for %s", sent, request.StreamId)

	return nil
}

func (p *Performer) protoToGocbConnectOpts(ctx context.Context, hostname string, auth gocb.Authenticator,
	cfg *shared.ClusterConfig, connID string) (string, gocb.ClusterOptions, error) {
	if cfg == nil {
		return hostname, gocb.ClusterOptions{
			Authenticator: auth,
		}, nil
	}

	var query []string
	opts := gocb.ClusterOptions{
		Authenticator: auth,
	}

	if cfg.CertPath == nil {
		if cfg.Cert == nil {
			// Setting TLSSkipVerify won't enable TLS so this is OK to do even if TLS isn't in use.
			opts.SecurityConfig.TLSSkipVerify = true
		} else {
			roots := x509.NewCertPool()
			ok := roots.AppendCertsFromPEM([]byte(cfg.GetCert()))
			if !ok {
				return "", gocb.ClusterOptions{}, errors.New("not a valid PEM")
			}
			opts.SecurityConfig.TLSRootCAs = roots
		}
	} else {
		roots := x509.NewCertPool()
		cacert, err := os.ReadFile(*cfg.CertPath)
		if err != nil {
			return "", gocb.ClusterOptions{}, err
		}

		ok := roots.AppendCertsFromPEM(cacert)
		if !ok {
			return "", gocb.ClusterOptions{}, errors.New("not a valid PEM")
		}
		opts.SecurityConfig.TLSRootCAs = roots
	}

	if cfg.KvConnectTimeoutSecs != nil {
		opts.TimeoutsConfig.ConnectTimeout = time.Duration(cfg.GetKvConnectTimeoutSecs()) * time.Second
	}
	if cfg.KvTimeoutMillis != nil {
		opts.TimeoutsConfig.KVTimeout = time.Duration(cfg.GetKvTimeoutMillis()) * time.Millisecond
	}
	if cfg.KvDurableTimeoutMillis != nil {
		opts.TimeoutsConfig.KVDurableTimeout = time.Duration(cfg.GetKvDurableTimeoutMillis()) * time.Millisecond
	}

	if cfg.KvScanTimeoutSecs != nil {
		opts.TimeoutsConfig.KVScanTimeout = time.Duration(cfg.GetKvScanTimeoutSecs()) * time.Second
	}

	if cfg.ViewTimeoutSecs != nil {
		opts.TimeoutsConfig.ViewTimeout = time.Duration(cfg.GetViewTimeoutSecs()) * time.Second
	}
	if cfg.QueryTimeoutSecs != nil {
		opts.TimeoutsConfig.QueryTimeout = time.Duration(cfg.GetQueryTimeoutSecs()) * time.Second
	}
	if cfg.AnalyticsTimeoutSecs != nil {
		opts.TimeoutsConfig.AnalyticsTimeout = time.Duration(cfg.GetAnalyticsTimeoutSecs()) * time.Second
	}
	if cfg.SearchTimeoutSecs != nil {
		opts.TimeoutsConfig.SearchTimeout = time.Duration(cfg.GetSearchTimeoutSecs()) * time.Second
	}
	if cfg.ManagementTimeoutSecs != nil {
		opts.TimeoutsConfig.ManagementTimeout = time.Duration(cfg.GetManagementTimeoutSecs()) * time.Second
	}
	if cfg.Transcoder != nil {
		var err error
		opts.Transcoder, err = helpers.Transcoder(cfg.Transcoder)
		if err != nil {
			return "", gocb.ClusterOptions{}, err
		}
		opts.IoConfig.DisableMutationTokens = !cfg.GetEnableMutationTokens()
		if cfg.ConfigPollIntervalSecs != nil {
			query = append(query, fmt.Sprintf("config_poll_interval=%d", (time.Duration(cfg.GetConfigPollIntervalSecs())*time.Second).Milliseconds()))
		}
		if cfg.NumKvConnections != nil {
			query = append(query, fmt.Sprintf("kv_poll_size=%d", cfg.GetNumKvConnections()))
		}
		if cfg.MaxHttpConnections != nil {
			query = append(query, fmt.Sprintf("max_idle_http_connections=%d", cfg.GetMaxHttpConnections()))
		}
	}

	if cfg.TransactionsConfig != nil {
		reqTxnsConfig := cfg.TransactionsConfig
		var keyspace *gocb.TransactionKeyspace
		if reqTxnsConfig.MetadataCollection != nil {
			keyspace = &gocb.TransactionKeyspace{
				BucketName:     reqTxnsConfig.MetadataCollection.BucketName,
				ScopeName:      reqTxnsConfig.MetadataCollection.ScopeName,
				CollectionName: reqTxnsConfig.MetadataCollection.CollectionName,
			}
		}
		var queryConfig gocb.TransactionsQueryConfig
		if reqTxnsConfig.QueryConfig != nil {
			queryConfig = gocb.TransactionsQueryConfig{
				ScanConsistency: helpers.ScanConsistencyToGocb(reqTxnsConfig.QueryConfig.GetScanConsistency()),
			}
		}
		var cleanupConfig gocb.TransactionsCleanupConfig
		if reqTxnsConfig.CleanupConfig != nil {
			collections := make([]gocb.TransactionKeyspace, len(reqTxnsConfig.CleanupConfig.GetCleanupCollection()))
			for i, col := range reqTxnsConfig.CleanupConfig.GetCleanupCollection() {
				collections[i] = gocb.TransactionKeyspace{
					BucketName:     col.BucketName,
					ScopeName:      col.ScopeName,
					CollectionName: col.CollectionName,
				}
			}

			cleanupConfig = gocb.TransactionsCleanupConfig{
				CleanupWindow:               time.Duration(reqTxnsConfig.CleanupConfig.GetCleanupWindowMillis()) * time.Millisecond,
				DisableClientAttemptCleanup: !reqTxnsConfig.CleanupConfig.GetCleanupClientAttempts(),
				DisableLostAttemptCleanup:   !reqTxnsConfig.CleanupConfig.GetCleanupLostAttempts(),
				CleanupQueueSize:            0,
				CleanupCollections:          collections,
			}
		}

		opts.TransactionsConfig = gocb.TransactionsConfig{
			MetadataCollection: keyspace,
			Timeout:            time.Duration(reqTxnsConfig.GetTimeoutMillis()) * time.Millisecond,
			DurabilityLevel:    helpers.ProtocolDuraToSDK(reqTxnsConfig.GetDurability()),
			QueryConfig:        queryConfig,
			CleanupConfig:      cleanupConfig,
		}
	}

	if cfg.ObservabilityConfig != nil {
		if cfg.ObservabilityConfig.OrphanResponse != nil {
			oConfig := cfg.ObservabilityConfig.OrphanResponse
			if oConfig.EmitIntervalMillis != nil {
				opts.OrphanReporterConfig.ReportInterval = time.Duration(*oConfig.EmitIntervalMillis) * time.Millisecond
			}
			if oConfig.SampleSize != nil {
				opts.OrphanReporterConfig.SampleSize = uint32(*oConfig.SampleSize)
			}
			if oConfig.Enabled != nil {
				opts.OrphanReporterConfig.Disabled = !(*oConfig.Enabled)
			}
		}
		tracer, err := p.createTracer(ctx, cfg.ObservabilityConfig, connID)
		if err != nil {
			return "", gocb.ClusterOptions{}, err
		}
		opts.Tracer = tracer

		meter, err := p.createMeter(ctx, cfg.ObservabilityConfig, connID)
		if err != nil {
			return "", gocb.ClusterOptions{}, err
		}
		opts.Meter = meter
	}

	if cfg.PreferredServerGroup != nil {
		opts.PreferredServerGroup = cfg.GetPreferredServerGroup()
	}

	if cfg.AppTelemetryEndpoint != nil {
		opts.AppTelemetryConfig.ExternalEndpoint = cfg.GetAppTelemetryEndpoint()
	}
	if cfg.AppTelemetryBackoffSecs != nil {
		opts.AppTelemetryConfig.Backoff = time.Duration(cfg.GetAppTelemetryBackoffSecs()) * time.Second
	}
	if cfg.EnableAppTelemetry != nil {
		opts.AppTelemetryConfig.Disabled = !cfg.GetEnableAppTelemetry()
	}
	if cfg.AppTelemetryPingTimeoutSecs != nil {
		opts.AppTelemetryConfig.PingTimeout = time.Duration(cfg.GetAppTelemetryPingTimeoutSecs()) * time.Second
	}
	if cfg.AppTelemetryPingIntervalSecs != nil {
		opts.AppTelemetryConfig.PingInterval = time.Duration(cfg.GetAppTelemetryPingIntervalSecs()) * time.Second
	}

	for _, conv := range cfg.ObservabilityConfig.GetObservabilitySemanticConventionOptIn() {
		switch conv {
		case observability.SemanticConvention_DATABASE:
			opts.ObservabilityConfig.SemanticConventionOptIn =
				append(opts.ObservabilityConfig.SemanticConventionOptIn, gocb.ObservabilitySemanticConventionDatabase)
		case observability.SemanticConvention_DATABASE_DUP:
			opts.ObservabilityConfig.SemanticConventionOptIn =
				append(opts.ObservabilityConfig.SemanticConventionOptIn, gocb.ObservabilitySemanticConventionDatabaseDup)
		default:
			return "", gocb.ClusterOptions{}, status.Errorf(codes.InvalidArgument, "unexpected semantic convention opt-in: %s", conv.String())
		}
	}

	return hostname + "?" + strings.Join(query, "&"), opts, nil
}

func (p *Performer) createMeter(ctx context.Context, config *observability.Config, connID string) (gocb.Meter, error) {
	if config.LoggingMeter != nil {
		opts := &gocb.LoggingMeterOptions{}
		if config.LoggingMeter.EmitIntervalMillis != nil {
			opts.EmitInterval = time.Duration(*config.LoggingMeter.EmitIntervalMillis) * time.Millisecond
		}

		return gocb.NewLoggingMeter(opts), nil
	}

	if config.Metrics != nil {
		certPool, err := telemetry.GetTelemetryServerCertPool()
		if err != nil {
			return nil, err
		}
		exporter, err := otlpmetricgrpc.New(
			ctx,
			otlpmetricgrpc.WithEndpointURL(config.Tracing.EndpointHostname),
			otlpmetricgrpc.WithCompressor("gzip"),
			otlpmetricgrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(certPool, "")),
		)
		if err != nil {
			return nil, err
		}

		var providerOpts []metricsdk.Option

		if len(config.Metrics.Resources) > 0 {
			var attributes []attribute.KeyValue
			for k, attrib := range config.Metrics.Resources {
				switch r := attrib.Value.(type) {
				case *observability.Attribute_ValueBoolean:
					attributes = append(attributes, attribute.Bool(k, r.ValueBoolean))
				case *observability.Attribute_ValueString:
					attributes = append(attributes, attribute.String(k, r.ValueString))
				case *observability.Attribute_ValueLong:
					attributes = append(attributes, attribute.Int64(k, r.ValueLong))
				}
			}

			res, err := resource.New(ctx,
				resource.WithAttributes(
					attributes...,
				),
			)
			if err != nil {
				return nil, err
			}

			providerOpts = append(providerOpts, metricsdk.WithResource(res))
		}

		reader := metricsdk.NewPeriodicReader(
			exporter,
			metricsdk.WithInterval(time.Duration(config.Metrics.ExportEveryMillis)*time.Millisecond),
		)
		providerOpts = append(providerOpts, metricsdk.WithReader(reader))

		meterProvider := metricsdk.NewMeterProvider(providerOpts...)
		p.telemetryLock.Lock()
		p.meterProviders[connID] = meterProvider
		p.telemetryLock.Unlock()

		meter := gocbopentelemetry.NewOpenTelemetryMeter(meterProvider)

		return meter, nil
	}

	return nil, nil
}

func (p *Performer) createTracer(ctx context.Context, config *observability.Config, connID string) (gocb.RequestTracer, error) {
	if config.UseNoopTracer {
		return &gocb.NoopTracer{}, nil
	}

	if config.ThresholdLoggingTracer != nil {
		thresholdConfig := config.ThresholdLoggingTracer
		opts := &gocb.ThresholdLoggingOptions{}
		if thresholdConfig.EmitIntervalMillis != nil {
			opts.Interval = time.Duration(*thresholdConfig.EmitIntervalMillis) * time.Millisecond
		}
		if thresholdConfig.SampleSize != nil {
			opts.SampleSize = uint32(*thresholdConfig.SampleSize)
		}
		if thresholdConfig.AnalyticsThresholdMillis != nil {
			opts.AnalyticsThreshold = time.Duration(*thresholdConfig.AnalyticsThresholdMillis) * time.Millisecond
		}
		if thresholdConfig.QueryThresholdMillis != nil {
			opts.QueryThreshold = time.Duration(*thresholdConfig.QueryThresholdMillis) * time.Millisecond
		}
		if thresholdConfig.KvThresholdMillis != nil {
			opts.KVThreshold = time.Duration(*thresholdConfig.KvThresholdMillis) * time.Millisecond
		}
		if thresholdConfig.SearchThresholdMillis != nil {
			opts.SearchThreshold = time.Duration(*thresholdConfig.SearchThresholdMillis) * time.Millisecond
		}
		if thresholdConfig.ViewsThresholdMillis != nil {
			opts.ViewsThreshold = time.Duration(*thresholdConfig.ViewsThresholdMillis) * time.Millisecond
		}

		return gocb.NewThresholdLoggingTracer(opts), nil
	}

	if config.Tracing != nil {
		// Set up a trace exporter
		certPool, err := telemetry.GetTelemetryServerCertPool()
		if err != nil {
			return nil, err
		}
		traceExporter, err := otlptracegrpc.New(
			ctx,
			otlptracegrpc.WithEndpointURL(config.Tracing.EndpointHostname),
			otlptracegrpc.WithCompressor("gzip"),
			otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(certPool, "")),
		)
		if err != nil {
			return nil, err
		}

		var tracerProviderOpts []sdktrace.TracerProviderOption

		if len(config.Tracing.Resources) > 0 {
			var attributes []attribute.KeyValue
			for k, attrib := range config.Tracing.Resources {
				switch r := attrib.Value.(type) {
				case *observability.Attribute_ValueBoolean:
					attributes = append(attributes, attribute.Bool(k, r.ValueBoolean))
				case *observability.Attribute_ValueString:
					attributes = append(attributes, attribute.String(k, r.ValueString))
				case *observability.Attribute_ValueLong:
					attributes = append(attributes, attribute.Int64(k, r.ValueLong))
				}
			}

			res, err := resource.New(ctx,
				resource.WithAttributes(
					attributes...,
				),
			)
			if err != nil {
				return nil, err
			}

			tracerProviderOpts = append(tracerProviderOpts, sdktrace.WithResource(res))
		}

		if config.Tracing.Batching {
			bsp := sdktrace.NewBatchSpanProcessor(traceExporter, sdktrace.WithBatchTimeout(time.Duration(config.Tracing.ExportEveryMillis)*time.Millisecond))
			tracerProviderOpts = append(tracerProviderOpts, sdktrace.WithSpanProcessor(bsp))
		} else {
			tracerProviderOpts = append(tracerProviderOpts, sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(traceExporter)))
		}

		samplingPercentage := config.Tracing.SamplingPercentage
		var sampler sdktrace.Sampler
		var epsilon float32 = 0.00001
		if samplingPercentage <= epsilon {
			sampler = sdktrace.NeverSample()
		} else if samplingPercentage >= 1.0-epsilon {
			sampler = sdktrace.AlwaysSample()
		} else {
			sampler = sdktrace.TraceIDRatioBased(float64(samplingPercentage))
		}
		tracerProviderOpts = append(tracerProviderOpts, sdktrace.WithSampler(sampler))

		tracerProvider := sdktrace.NewTracerProvider(
			tracerProviderOpts...,
		)

		// Set global propagator to tracecontext (the default is no-op).
		// This enables tracing across the grpc layer.
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

		p.telemetryLock.Lock()
		p.tracerProviders[connID] = tracerProvider
		p.telemetryLock.Unlock()

		tracer := gocbopentelemetry.NewOpenTelemetryRequestTracer(tracerProvider)
		return tracer, nil
	}

	return nil, nil
}
