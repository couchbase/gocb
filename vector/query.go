package vector

import (
	"encoding/json"
	"errors"
)

// Query specifies a vector Query.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
type Query struct {
	field  string
	vector []float32

	numCandidates *uint32
	boost         *float32
}

// NewQuery constructs a new vector Query.
//
// # VOLATILE
//
// This API is VOLATILE and subject to change at any time.
func NewQuery(vectorFieldName string, vector []float32) *Query {
	return &Query{
		field:  vectorFieldName,
		vector: vector,
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

// InternalQuery is used for internal functionality.
// Internal: This should never be used and is not supported.
type InternalQuery struct {
	Field  string
	Vector []float32

	NumCandidates *uint32
	Boost         *float32
}

// Internal is used for internal functionality.
// Internal: This should never be used and is not supported.
func (q *Query) Internal() InternalQuery {
	return InternalQuery{
		Field:         q.field,
		Vector:        q.vector,
		NumCandidates: q.numCandidates,
		Boost:         q.boost,
	}
}

// Validate verifies that settings in the query are valid.
func (q InternalQuery) Validate() error {
	if len(q.Field) == 0 {
		return errors.New("vectorFieldName cannot be empty")
	}
	if len(q.Vector) == 0 {
		return errors.New("vector cannot be empty")
	}
	if q.NumCandidates != nil && *q.NumCandidates == 0 {
		return errors.New("when set numCandidates must have a value >= 1")
	}

	return nil
}

// MarshalJSON marshal's this query to JSON for the search REST API.
func (q InternalQuery) MarshalJSON() ([]byte, error) {
	outStruct := &struct {
		Field         string    `json:"field"`
		Vector        []float32 `json:"vector"`
		NumCandidates *uint32   `json:"k,omitempty"`
		Boost         *float32  `json:"boost,omitempty"`
	}{
		Field:         q.Field,
		Vector:        q.Vector,
		NumCandidates: q.NumCandidates,
		Boost:         q.Boost,
	}

	return json.Marshal(outStruct)
}
