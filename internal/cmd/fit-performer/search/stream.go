package search

import (
	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/streams"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/search"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

type SearchStreamItem struct {
	wrapped        gocb.SearchRow
	fieldContentAs *shared.ContentAs
}

func (item *SearchStreamItem) IsStreamItem() {}

func (item *SearchStreamItem) Row() (*search.SearchRow, error) {
	return ParseSearchRow(item.wrapped, item.fieldContentAs)
}

type SearchStream struct {
	stream         *gocb.SearchResult
	fieldContentAs *shared.ContentAs
	runID          string
	completed      chan struct{}
	streamID       string
}

type SearchStreamOptions struct {
	FieldContentAs *shared.ContentAs
}

func NewSearchStream(stream *gocb.SearchResult, streamID, runID string, opts *SearchStreamOptions) *SearchStream {
	if opts == nil {
		opts = &SearchStreamOptions{}
	}

	return &SearchStream{
		stream:         stream,
		fieldContentAs: opts.FieldContentAs,
		streamID:       streamID,
		runID:          runID,
		completed:      make(chan struct{}),
	}
}

func (r *SearchStream) RunID() string {
	return r.runID
}

func (r *SearchStream) Completed() <-chan struct{} {
	return r.completed
}

func (r *SearchStream) Next() streams.StreamItem {
	hasNext := r.stream.Next()
	if !hasNext {
		// We're returning an interface which we shouldn't really so we have to tell Go that this is
		// really nil.
		return nil
	}

	return &SearchStreamItem{
		wrapped:        r.stream.Row(),
		fieldContentAs: r.fieldContentAs,
	}
}

func (r *SearchStream) Metadata() (*search.SearchMetaData, error) {
	meta, err := r.stream.MetaData()
	if err != nil {
		return nil, err
	}
	return ParseSearchResultMeta(meta)
}

func (r *SearchStream) Facets() (*search.SearchFacets, error) {
	facets, err := r.stream.Facets()
	if err != nil {
		return nil, err
	}
	return ParseSearchResultFacets(facets)
}

func (r *SearchStream) Err() error {
	return r.stream.Err()
}

func (r *SearchStream) Cancel() error {
	return r.stream.Close()
}

func (r *SearchStream) Finish() {
	close(r.completed)
}
