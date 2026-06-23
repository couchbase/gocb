package getreplicas

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/streams"
)

type GetAllReplicasResultItem struct {
	wrapped   *gocb.GetReplicaResult
	contentAs *shared.ContentAs
}

func (item *GetAllReplicasResultItem) IsReplica() bool {
	return item.wrapped.IsReplica()
}

func (item *GetAllReplicasResultItem) Cas() *int64 {
	cas := int64(item.wrapped.Cas())
	if cas == 0 {
		return nil
	}
	return &cas
}

func (item *GetAllReplicasResultItem) GetResult() gocb.GetResult {
	return item.wrapped.GetResult
}

func (item *GetAllReplicasResultItem) ContentAs() *shared.ContentAs {
	return item.contentAs
}

func (item *GetAllReplicasResultItem) IsStreamItem() {}

type GetAllReplicasStream struct {
	contentAs *shared.ContentAs
	stream    *gocb.GetAllReplicasResult
	runID     string
	completed chan struct{}
	streamID  string
	error
}

func (r *GetAllReplicasStream) Err() error {
	return nil
}

func NewGetAllReplicasStream(stream *gocb.GetAllReplicasResult, streamID, runID string, contentAs *shared.ContentAs) *GetAllReplicasStream {
	return &GetAllReplicasStream{
		contentAs: contentAs,
		stream:    stream,
		streamID:  streamID,
		runID:     runID,
		completed: make(chan struct{}),
	}
}

func (r *GetAllReplicasStream) RunID() string {
	return r.runID
}

func (r *GetAllReplicasStream) Completed() <-chan struct{} {
	return r.completed
}

func (r *GetAllReplicasStream) Next() streams.StreamItem {
	item := r.stream.Next()
	if item == nil {
		// We're returning an interface which we shouldn't really so we have to tell Go that this is
		// really nil.
		return nil
	}
	return &GetAllReplicasResultItem{
		wrapped:   item,
		contentAs: r.contentAs,
	}
}

func (r *GetAllReplicasStream) Cancel() error {
	return r.stream.Close()
}

func (r *GetAllReplicasStream) Finish() {
	close(r.completed)
}
