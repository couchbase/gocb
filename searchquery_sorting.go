package gocb

import (
	"encoding/json"
)

// FtsSort represents an FTS sorting for a search query.
type FtsSort interface {
}

type ftsSortBase struct {
	options map[string]interface{}
}

func newFtsSortBase() ftsSortBase {
	return ftsSortBase{
		options: make(map[string]interface{}),
	}
}

// MarshalJSON marshal's this query to JSON for the FTS REST API.
func (q ftsSortBase) MarshalJSON() ([]byte, error) {
	return json.Marshal(q.options)
}

// SearchSortScore represents a FTS score sort.
type SearchSortScore struct {
	ftsSortBase
}

// NewSearchSortScore creates a new SearchSortScore.
func NewSearchSortScore() *SearchSortScore {
	q := &SearchSortScore{newFtsSortBase()}
	q.options["by"] = "score"
	return q
}

// Descending specifies the ordering of the results.
func (q *SearchSortScore) Descending(descending bool) *SearchSortScore {
	q.options["desc"] = descending
	return q
}

// SearchSortId represents a FTS Document ID sort.
type SearchSortId struct {
	ftsSortBase
}

// NewSearchSortId creates a new SearchSortScore.
func NewSearchSortId() *SearchSortId {
	q := &SearchSortId{newFtsSortBase()}
	q.options["by"] = "id"
	return q
}

// Descending specifies the ordering of the results.
func (q *SearchSortId) Descending(descending bool) *SearchSortId {
	q.options["desc"] = descending
	return q
}

// SearchSortField represents a FTS field sort.
type SearchSortField struct {
	ftsSortBase
}

// NewSearchSortField creates a new SearchSortField.
func NewSearchSortField(field string) *SearchSortField {
	q := &SearchSortField{newFtsSortBase()}
	q.options["by"] = "field"
	q.options["field"] = field
	return q
}

// Type allows you to specify the FTS field sort type.
func (q *SearchSortField) Type(value string) *SearchSortField {
	q.options["type"] = value
	return q
}

// Mode allows you to specify the FTS field sort mode.
func (q *SearchSortField) Mode(mode string) *SearchSortField {
	q.options["mode"] = mode
	return q
}

// Missing allows you to specify the FTS field sort missing behaviour.
func (q *SearchSortField) Missing(missing string) *SearchSortField {
	q.options["missing"] = missing
	return q
}

// Descending specifies the ordering of the results.
func (q *SearchSortField) Descending(descending bool) *SearchSortField {
	q.options["desc"] = descending
	return q
}

// SearchSortGeoDistance represents a FTS geo sort.
type SearchSortGeoDistance struct {
	ftsSortBase
}

// NewSearchSortGeoDistance creates a new SearchSortGeoDistance.
func NewSearchSortGeoDistance(field string, lat, lon float64) *SearchSortGeoDistance {
	q := &SearchSortGeoDistance{newFtsSortBase()}
	q.options["by"] = "geo_distance"
	q.options["field"] = field
	q.options["location"] = []float64{lon, lat}
	return q
}

// Unit specifies the unit used for sorting
func (q *SearchSortGeoDistance) Unit(unit string) *SearchSortGeoDistance {
	q.options["unit"] = unit
	return q
}

// Descending specifies the ordering of the results.
func (q *SearchSortGeoDistance) Descending(descending bool) *SearchSortGeoDistance {
	q.options["desc"] = descending
	return q
}
