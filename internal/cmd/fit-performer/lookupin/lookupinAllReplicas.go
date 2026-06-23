package lookupin

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/lookupin"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/streams"
)

type LookupInAllReplicasStreamItem struct {
	wrapped *gocb.LookupInReplicaResult
	specs   []*lookupin.LookupInSpec
}

func (item *LookupInAllReplicasStreamItem) IsStreamItem() {}

func (item *LookupInAllReplicasStreamItem) HasNext(idx uint) bool {
	return idx < uint(len(item.specs))
}

func (item *LookupInAllReplicasStreamItem) contentAs(idx uint) *shared.ContentAs {
	return item.specs[idx].ContentAs
}

func (item *LookupInAllReplicasStreamItem) ContentAt(idx uint) (*shared.ContentTypes, error) {
	return helpers.ParseContentAs(item.contentAs(idx), func(content interface{}) error {
		return item.wrapped.ContentAt(idx, content)
	})
}

func (item *LookupInAllReplicasStreamItem) Cas() int64 {
	return int64(item.wrapped.Cas())
}

func (item *LookupInAllReplicasStreamItem) Exists(idx uint) bool {
	exists := item.wrapped.Exists(idx)
	return exists
}

func (item *LookupInAllReplicasStreamItem) IsReplica() bool {
	return item.wrapped.IsReplica()
}

type LookupInAllReplicasStream struct {
	stream    *gocb.LookupInAllReplicasResult
	runID     string
	completed chan struct{}
	streamID  string

	currentSpec int
	specs       []*lookupin.LookupInSpec
}

func NewLookupInAllReplicasStreamStream(
	stream *gocb.LookupInAllReplicasResult,
	streamID, runID string,
	specs []*lookupin.LookupInSpec) *LookupInAllReplicasStream {
	return &LookupInAllReplicasStream{
		stream:    stream,
		streamID:  streamID,
		runID:     runID,
		completed: make(chan struct{}),
		specs:     specs,
	}
}

func (r *LookupInAllReplicasStream) RunID() string {
	return r.runID
}

func (r *LookupInAllReplicasStream) Completed() <-chan struct{} {
	return r.completed
}

func (r *LookupInAllReplicasStream) Next() streams.StreamItem {
	item := r.stream.Next()
	if item == nil {
		// We're returning an interface which we shouldn't really so we have to tell Go that this is
		// really nil.
		return nil
	}

	i := &LookupInAllReplicasStreamItem{
		wrapped: item,
		specs:   r.specs,
	}
	r.currentSpec++

	return i
}

func (r *LookupInAllReplicasStream) Err() error {
	return nil
}

func (r *LookupInAllReplicasStream) Cancel() error {
	return r.stream.Close()
}

func (r *LookupInAllReplicasStream) Finish() {
	close(r.completed)
}

func ParseLookupInAllItem(next streams.StreamItem) *lookupin.LookupInReplicaResult {
	item := next.(*LookupInAllReplicasStreamItem) //nolint:errcheck

	var thisResults []*lookupin.LookupInSpecResult
	var idx uint
	for item.HasNext(idx) {
		thisResult := &lookupin.LookupInSpecResult{
			ExistsResult: &lookupin.BooleanOrError{
				Result: &lookupin.BooleanOrError_Value{
					Value: item.Exists(idx),
				},
			},
		}

		content, err := item.ContentAt(idx)
		idx++
		if err != nil {
			thisResult.ContentAsResult = &shared.ContentOrError{
				Result: &shared.ContentOrError_Exception{
					Exception: helpers.MapErrorToProto(err),
				},
			}
			thisResults = append(thisResults, thisResult)
			continue
		}

		thisResult.ContentAsResult = &shared.ContentOrError{
			Result: &shared.ContentOrError_Content{
				Content: content,
			},
		}

		thisResults = append(thisResults, thisResult)
	}

	return &lookupin.LookupInReplicaResult{
		Results:   thisResults,
		IsReplica: item.IsReplica(),
	}
}
