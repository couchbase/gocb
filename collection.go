package gocb

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
)

// Collection represents a single collection.
type Collection struct {
	sb stateBlock
}

// CollectionOptions are the options available when opening a collection.
type CollectionOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

func newCollection(scope *Scope, collectionName string, opts *CollectionOptions) *Collection {
	if opts == nil {
		opts = &CollectionOptions{}
	}

	collection := &Collection{
		sb: scope.stateBlock(),
	}
	collection.sb.CollectionName = collectionName
	collection.sb.cacheClient()

	span := collection.startKvOpTrace(opts.ParentSpanContext, "GetCollectionID")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := collection.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	cli := collection.sb.getCachedClient()
	cli.openCollection(deadlinedCtx, span.Context(), collection.sb.ScopeName, collection.sb.CollectionName)

	return collection
}

func (c *Collection) name() string {
	return c.sb.CollectionName
}

func (c *Collection) scopeName() string {
	return c.sb.ScopeName
}

func (c *Collection) clone() *Collection {
	newC := *c
	return &newC
}

func (c *Collection) getKvProvider() (kvProvider, error) {
	cli := c.sb.getCachedClient()
	agent, err := cli.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent, nil
}

// startKvOpTrace starts a new span for a given operationName. If parentSpanCtx is not nil then the span will be a
// ChildOf that span context.
func (c *Collection) startKvOpTrace(parentSpanCtx opentracing.SpanContext, operationName string) opentracing.Span {
	var span opentracing.Span
	if parentSpanCtx == nil {
		span = opentracing.GlobalTracer().StartSpan(operationName,
			opentracing.Tag{Key: "couchbase.collection", Value: c.sb.CollectionName},
			opentracing.Tag{Key: "couchbase.service", Value: "kv"})
	} else {
		span = opentracing.GlobalTracer().StartSpan(operationName,
			opentracing.Tag{Key: "couchbase.collection", Value: c.sb.CollectionName},
			opentracing.Tag{Key: "couchbase.service", Value: "kv"}, opentracing.ChildOf(parentSpanCtx))
	}

	return span
}
