package vector

import (
	"errors"
)

// VectorQueryCombination specifies how elements in the array are combined.
//
// # UNCOMMITTED
//
// This API is UNCOMMITTED and may change in the future.
type VectorQueryCombination string

const (
	VectorQueryCombinationNotSet VectorQueryCombination = ""
	VectorQueryCombinationAnd    VectorQueryCombination = "and"
	VectorQueryCombinationOr     VectorQueryCombination = "or"
)

// SearchOptions specifies the options available to vector Search.
//
// # UNCOMMITTED
//
// This API is UNCOMMITTED and may change in the future.
type SearchOptions struct {
	VectorQueryCombination VectorQueryCombination
}

// Search specifies a vector Search.
//
// # UNCOMMITTED
//
// This API is UNCOMMITTED and may change in the future.
type Search struct {
	queries []*Query

	vectorQueryCombination VectorQueryCombination
}

// NewSearch constructs a new vector Search.
//
// # UNCOMMITTED
//
// This API is UNCOMMITTED and may change in the future.
func NewSearch(queries []*Query, opts *SearchOptions) *Search {
	if opts == nil {
		opts = &SearchOptions{}
	}

	return &Search{
		queries:                queries,
		vectorQueryCombination: opts.VectorQueryCombination,
	}
}

// InternalSearch is used for internal functionality.
// Internal: This should never be used and is not supported.
type InternalSearch struct {
	Queries []InternalQuery

	VectorQueryCombination VectorQueryCombination
}

// Internal is used for internal functionality.
// Internal: This should never be used and is not supported.
func (s *Search) Internal() InternalSearch {
	queries := make([]InternalQuery, len(s.queries))
	for i, query := range s.queries {
		queries[i] = query.Internal()
	}
	return InternalSearch{
		Queries:                queries,
		VectorQueryCombination: s.vectorQueryCombination,
	}
}

// Validate verifies that settings in the search (including all queries) are valid.
func (s InternalSearch) Validate() error {
	if len(s.Queries) == 0 {
		return errors.New("queries cannot be empty")
	}
	for _, query := range s.Queries {
		if err := query.Validate(); err != nil {
			return err
		}
	}

	return nil
}
