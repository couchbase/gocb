package gocb

import (
	"github.com/couchbase/gocbcore/v10"
	"go.opentelemetry.io/otel/trace"
	"slices"
	"time"
)

func tracerAddRef(tracer RequestTracer) {
	if tracer == nil {
		return
	}
	if refTracer, ok := tracer.(interface {
		AddRef() int32
	}); ok {
		refTracer.AddRef()
	}
}

func tracerDecRef(tracer RequestTracer) {
	if tracer == nil {
		return
	}
	if refTracer, ok := tracer.(interface {
		DecRef() int32
	}); ok {
		refTracer.DecRef()
	}
}

// RequestTracer describes the tracing abstraction in the SDK.
type RequestTracer interface {
	RequestSpan(parentContext RequestSpanContext, operationName string) RequestSpan
}

type OtelAwareRequestTracer interface {
	Wrapped() trace.Tracer
	Provider() trace.TracerProvider
}

// RequestSpan is the interface for spans that are created by a RequestTracer.
type RequestSpan interface {
	End()
	Context() RequestSpanContext
	AddEvent(name string, timestamp time.Time)
	SetAttribute(key string, value interface{})
}

// RequestSpanContext is the interface for external span contexts that can be passed in into the SDK option blocks.
type RequestSpanContext interface {
}

type OtelAwareRequestSpan interface {
	Wrapped() trace.Span
}

type coreRequestTracerWrapper struct {
	tracer RequestTracer
}

func (tracer *coreRequestTracerWrapper) RequestSpan(parentContext gocbcore.RequestSpanContext, operationName string) gocbcore.RequestSpan {
	return &coreRequestSpanWrapper{
		span: tracer.tracer.RequestSpan(parentContext, operationName),
	}
}

type coreRequestSpanWrapper struct {
	span RequestSpan
}

func (span *coreRequestSpanWrapper) End() {
	span.span.End()
}

func (span *coreRequestSpanWrapper) Context() gocbcore.RequestSpanContext {
	return span.span.Context()
}

func (span *coreRequestSpanWrapper) SetAttribute(key string, value interface{}) {
	span.span.SetAttribute(key, value)
}

func (span *coreRequestSpanWrapper) AddEvent(key string, timestamp time.Time) {
	span.span.SetAttribute(key, timestamp)
}

type noopSpan struct{}
type noopSpanContext struct{}

var (
	defaultNoopSpanContext = noopSpanContext{}
	defaultNoopSpan        = noopSpan{}
)

// NoopTracer is a RequestTracer implementation that does not perform any tracing.
type NoopTracer struct { //nolint:unused
}

// RequestSpan creates a new RequestSpan.
func (tracer *NoopTracer) RequestSpan(parentContext RequestSpanContext, operationName string) RequestSpan {
	return defaultNoopSpan
}

// End completes the span.
func (span noopSpan) End() {
}

// Context returns the RequestSpanContext for this span.
func (span noopSpan) Context() RequestSpanContext {
	return defaultNoopSpanContext
}

// SetAttribute adds an attribute to this span.
func (span noopSpan) SetAttribute(key string, value interface{}) {
}

// AddEvent adds an event to this span.
func (span noopSpan) AddEvent(key string, timestamp time.Time) {
}

type tracerWrapper struct {
	tracer                   RequestTracer
	clusterLabelsProvider    clusterLabelsProvider
	includeLegacyConventions bool
	includeStableConventions bool
}

func newTracerWrapper(tracer RequestTracer, config ObservabilityConfig) *tracerWrapper {
	var includeLegacy, includeStable bool
	if slices.Contains(config.SemanticConventionOptIn, ObservabilitySemanticConventionDatabaseDup) {
		includeLegacy = true
		includeStable = true
	} else if slices.Contains(config.SemanticConventionOptIn, ObservabilitySemanticConventionDatabase) {
		includeStable = true
	} else {
		includeLegacy = true
	}

	return &tracerWrapper{
		tracer:                   tracer,
		includeLegacyConventions: includeLegacy,
		includeStableConventions: includeStable,
	}
}

func (tw *tracerWrapper) createSpanInternal(parent RequestSpan, operationType, service string) *spanWrapper {
	var tracectx RequestSpanContext
	if parent != nil {
		tracectx = parent.Context()
	}

	span := tw.tracer.RequestSpan(tracectx, operationType)
	sw := &spanWrapper{
		span:                     span,
		includeLegacyConventions: tw.includeLegacyConventions,
		includeStableConventions: tw.includeStableConventions,
	}
	sw.SetSystemName(spanAttribSystemNameValue)
	if service != "" {
		sw.SetService(service)
	}
	if tw.clusterLabelsProvider != nil {
		labels := tw.clusterLabelsProvider.ClusterLabels()
		if labels.ClusterName != "" {
			sw.SetClusterName(labels.ClusterName)
		}
		if labels.ClusterUUID != "" {
			sw.SetClusterUUID(labels.ClusterUUID)
		}
	}

	return sw
}

func (tw *tracerWrapper) CreateOperationSpan(parent RequestSpan, operationName, service string) *spanWrapper {
	sw := tw.createSpanInternal(parent, operationName, service)
	sw.SetStableOperationName(operationName)
	return sw
}

func (tw *tracerWrapper) CreateSubOperationSpan(parent *spanWrapper, operationName, service string) *spanWrapper {
	sw := tw.createSpanInternal(parent.Wrapped(), operationName, service)
	sw.SetStableOperationName(operationName)
	return sw
}

func (tw *tracerWrapper) CreateRequestEncodingSpan(parent *spanWrapper) *spanWrapper {
	sw := tw.createSpanInternal(parent.Wrapped(), "request_encoding", "")
	return sw
}

type spanWrapper struct {
	span                     RequestSpan
	includeLegacyConventions bool
	includeStableConventions bool
}

func (sw *spanWrapper) Wrapped() RequestSpan {
	return sw.span
}

func (sw *spanWrapper) Context() RequestSpanContext {
	return sw.Wrapped().Context()
}

func (sw *spanWrapper) SetLegacyOperationName(v string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribOperationName, v)
	}
}

func (sw *spanWrapper) SetStableOperationName(v string) {
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribOperationName, v)
	}
}

func (sw *spanWrapper) SetSystemName(v string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribSystemName, v)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribSystemName, v)
	}
}

func (sw *spanWrapper) SetService(v string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribService, v)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribService, v)
	}
}

func (sw *spanWrapper) SetClusterName(v string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribClusterName, v)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribClusterName, v)
	}
}

func (sw *spanWrapper) SetClusterUUID(v string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribClusterUUID, v)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribClusterUUID, v)
	}
}

func (sw *spanWrapper) SetBucketName(v string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribBucketName, v)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribBucketName, v)
	}
}

func (sw *spanWrapper) SetScopeName(v string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribScopeName, v)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribScopeName, v)
	}
}

func (sw *spanWrapper) SetCollectionName(v string) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribCollectionName, v)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribCollectionName, v)
	}
}

func (sw *spanWrapper) SetQueryStatement(statement string, hasParameters bool) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribQueryStatement, statement)
	}
	if sw.includeStableConventions && hasParameters {
		sw.span.SetAttribute(spanStableAttribQueryStatement, statement)
	}
}

func (sw *spanWrapper) SetDurabilityLevel(level DurabilityLevel) {
	if sw.includeLegacyConventions && level > DurabilityLevelNone {
		// The encoding of the durability level wasn't following the RFC, but let's preserve the legacy behaviour.
		levelStr, err := level.toManagementAPI()
		if err != nil {
			logDebugf("Could not convert durability level to string: %v", err)
			return
		}
		sw.span.SetAttribute(spanLegacyAttribDurability, levelStr)
	}
	if sw.includeStableConventions {
		var levelStr string
		switch level {
		case DurabilityLevelUnknown, DurabilityLevelNone:
			break
		case DurabilityLevelMajority:
			levelStr = "majority"
		case DurabilityLevelMajorityAndPersistOnMaster:
			levelStr = "majority_and_persist_active"
		case DurabilityLevelPersistToMajority:
			levelStr = "persist_majority"
		}
		if levelStr != "" {
			sw.span.SetAttribute(spanStableAttribDurability, levelStr)
		}
	}
}

func (sw *spanWrapper) SetRetryAttempts(attempts uint32) {
	if sw.includeLegacyConventions {
		sw.span.SetAttribute(spanLegacyAttribNumRetries, attempts)
	}
	if sw.includeStableConventions {
		sw.span.SetAttribute(spanStableAttribNumRetries, attempts)
	}
}

func (sw *spanWrapper) SetRawAttribute(key string, value interface{}) {
	sw.span.SetAttribute(key, value)
}

func (sw *spanWrapper) End() {
	sw.span.End()
}
