package gocb

import (
	"context"
	"time"
)

// GetReplicaStrategy specifies which replica a GetReplica operation should read from.
// Use NewGetReplicaStrategyFromIndex to create one.
type GetReplicaStrategy struct {
	replicaIdx uint8
	wrap       bool
}

// ReplicaIndex represents the index of a replica, as configured on the bucket.
type ReplicaIndex uint8

const (
	ReplicaIndexFirst  ReplicaIndex = 1
	ReplicaIndexSecond ReplicaIndex = 2
	ReplicaIndexThird  ReplicaIndex = 3
)

// GetReplicaStrategyFromIndexOptions are the options available to the NewGetReplicaStrategyFromIndex function.
type GetReplicaStrategyFromIndexOptions struct {
	// Wrap controls what happens when the requested replica index is not currently available, or if it is out of bounds
	// (i.e. the server has fewer replicas than the requested index).
	// When false, the operation fails with ErrReplicaIndexOutOfBounds or ErrReplicaIndexCurrentlyUnavailable.
	// When true, replicas are walked starting from the requested index (wrapping around after the last one) until
	// an available replica is found. If no replicas are, the operation fails with ErrReplicaIndexCurrentlyUnavailable.
	Wrap bool
}

// NewGetReplicaStrategyFromIndex creates a GetReplicaStrategy which reads from the replica at the given index.
func NewGetReplicaStrategyFromIndex(idx ReplicaIndex, opts *GetReplicaStrategyFromIndexOptions) (GetReplicaStrategy, error) {
	if idx < ReplicaIndexFirst || idx > ReplicaIndexThird {
		return GetReplicaStrategy{}, makeInvalidArgumentsError(
			"invalid replica index, one of ReplicaIndexFirst, ReplicaIndexSecond, ReplicaIndexThird must be provided")
	}

	if opts == nil {
		opts = &GetReplicaStrategyFromIndexOptions{}
	}

	return GetReplicaStrategy{
		replicaIdx: uint8(idx),
		wrap:       opts.Wrap,
	}, nil
}

// GetReplicaOptions are options that can be applied to a GetReplica operation.
type GetReplicaOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// GetReplica returns the value of a particular document from a specific replica server, according
// to the provided GetReplicaStrategy
func (c *Collection) GetReplica(id string, strategy GetReplicaStrategy, opts *GetReplicaOptions) (*GetReplicaResult, error) {
	return autoOpControl(c.kvController(), "get_replica", func(agent kvProvider) (*GetReplicaResult, error) {
		if opts == nil {
			opts = &GetReplicaOptions{}
		}

		return agent.GetReplica(c, id, strategy, opts)
	})
}
