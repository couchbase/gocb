package gocb

import (
	"github.com/couchbase/gocb/v2/search"
	"github.com/couchbase/gocb/v2/vector"
)

// SearchRequest is used for describing a search request used with Search.
type SearchRequest struct {
	SearchQuery search.Query

	// # UNCOMMITTED
	//
	// This API is UNCOMMITTED and may change in the future.
	VectorSearch *vector.Search
}
