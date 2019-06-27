package gocb

import opentracing "github.com/opentracing/opentracing-go"

func startSpan(spanCtx opentracing.SpanContext, opName, serviceName string) opentracing.Span {
	var span opentracing.Span
	if spanCtx == nil {
		span = opentracing.GlobalTracer().StartSpan(opName,
			opentracing.Tag{Key: "couchbase.service", Value: serviceName})
	} else {
		span = opentracing.GlobalTracer().StartSpan(opName,
			opentracing.Tag{Key: "couchbase.service", Value: serviceName}, opentracing.ChildOf(spanCtx))
	}
	return span
}
