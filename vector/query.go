package vector

import (
	"encoding/json"
	"errors"
	"github.com/couchbase/gocb/v2/search"
)

// Query specifies a vector Query.
type Query struct {
	field        string
	vector       []float32
	base64Vector string

	numCandidates *uint32
	boost         *float32
	prefilter     search.Query
}

// NewQuery constructs a new vector Query.
func NewQuery(vectorFieldName string, vector []float32) *Query {
	return &Query{
		field:  vectorFieldName,
		vector: vector,
	}
}

// NewBase64Query constructs a new vector Query using
// a Base64-encoded sequence of little-endian IEEE 754 floats.
func NewBase64Query(vectorFieldName string, base64Vector string) *Query {
	return &Query{
		field:        vectorFieldName,
		base64Vector: base64Vector,
	}
}

// NumCandidates controls how many results are returned for this query.
func (q *Query) NumCandidates(num uint32) *Query {
	q.numCandidates = &num
	return q
}

// Boost specifies the boost for this query.
func (q *Query) Boost(boost float32) *Query {
	q.boost = &boost
	return q
}

// Prefilter specifies a search.Query to filter by before executing the query.
func (q *Query) Prefilter(query search.Query) *Query {
	q.prefilter = &query
	return q
}

// InternalQuery is used for internal functionality.
// Internal: This should never be used and is not supported.
type InternalQuery struct {
	Field        string
	Vector       []float32
	Base64Vector string

	NumCandidates *uint32
	Boost         *float32
	Prefilter     search.Query
}

// Internal is used for internal functionality.
// Internal: This should never be used and is not supported.
func (q *Query) Internal() InternalQuery {
	return InternalQuery{
		Field:         q.field,
		Vector:        q.vector,
		Base64Vector:  q.base64Vector,
		NumCandidates: q.numCandidates,
		Boost:         q.boost,
		Prefilter:     q.prefilter,
	}
}

// Validate verifies that settings in the query are valid.
func (q InternalQuery) Validate() error {
	if len(q.Field) == 0 {
		return errors.New("vectorFieldName cannot be empty")
	}
	if len(q.Vector) == 0 && len(q.Base64Vector) == 0 {
		return errors.New("one of vector or base64vector must be specified")
	}
	if q.NumCandidates != nil && *q.NumCandidates == 0 {
		return errors.New("when set numCandidates must have a value >= 1")
	}

	return nil
}

// MarshalJSON marshal's this query to JSON for the search REST API.
func (q InternalQuery) MarshalJSON() ([]byte, error) {
	outStruct := &struct {
		Field         string       `json:"field"`
		Vector        []float32    `json:"vector,omitempty"`
		Base64Vector  string       `json:"vector_base64,omitempty"`
		NumCandidates *uint32      `json:"k,omitempty"`
		Boost         *float32     `json:"boost,omitempty"`
		PreFilter     search.Query `json:"filter,omitempty"`
	}{
		Field:         q.Field,
		Vector:        q.Vector,
		Base64Vector:  q.Base64Vector,
		NumCandidates: q.NumCandidates,
		Boost:         q.Boost,
		PreFilter:     q.Prefilter,
	}

	return json.Marshal(outStruct)
}
