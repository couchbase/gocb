package rangescan

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/streams"
)

type RangeScanResultItem struct {
	wrapped *gocb.ScanResultItem
	opts    *RangeScanOptions
}

func (item *RangeScanResultItem) IsStreamItem() {}

func (item *RangeScanResultItem) ID() string {
	return item.wrapped.ID()
}

func (item *RangeScanResultItem) IDOnly() bool {
	return item.wrapped.IDOnly()
}

func (item *RangeScanResultItem) Cas() *int64 {
	cas := int64(item.wrapped.Cas())
	if cas == 0 {
		return nil
	}

	return &cas
}

func (item *RangeScanResultItem) Content() (*shared.ContentTypes, error) {
	return helpers.ParseContentAs(item.opts.ContentAs, func(content interface{}) error {
		return item.wrapped.Content(content)
	})
}

func (item *RangeScanResultItem) ExpiryTime() *int64 {
	expiry := item.wrapped.ExpiryTime()
	if expiry.IsZero() {
		return nil
	}

	expiryUnix := expiry.Unix()
	return &expiryUnix
}

type RangeScanStream struct {
	stream    *gocb.ScanResult
	opts      *RangeScanOptions
	runID     string
	completed chan struct{}
	streamID  string
}

type RangeScanOptions struct {
	IDsOnly   bool
	ContentAs *shared.ContentAs
}

func NewRangeScanStream(stream *gocb.ScanResult, streamID, runID string, opts *RangeScanOptions) *RangeScanStream {
	if opts == nil {
		opts = &RangeScanOptions{}
	}
	return &RangeScanStream{
		stream:    stream,
		opts:      opts,
		streamID:  streamID,
		runID:     runID,
		completed: make(chan struct{}),
	}
}

func (r *RangeScanStream) RunID() string {
	return r.runID
}

func (r *RangeScanStream) Completed() <-chan struct{} {
	return r.completed
}

func (r *RangeScanStream) Next() streams.StreamItem {
	item := r.stream.Next()
	if item == nil {
		// We're returning an interface which we shouldn't really so we have to tell Go that this is
		// really nil.
		return nil
	}

	return &RangeScanResultItem{
		wrapped: item,
		opts:    r.opts,
	}
}

func (r *RangeScanStream) Err() error {
	return r.stream.Err()
}

func (r *RangeScanStream) Cancel() error {
	return r.stream.Close()
}

func (r *RangeScanStream) Finish() {
	close(r.completed)
}
