package gocb

import (
	"encoding/json"
)

type FtsFacet interface {
	json.Marshaler
	validate()
}

type termFacetData struct {
	Field string `json:"field,omitempty"`
	Size  int    `json:"size,omitempty"`
}
type TermFacet struct {
	data termFacetData
}

func (f TermFacet) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.data)
}

func (f TermFacet) validate() {
}

func NewTermFacet(field string, size int) *TermFacet {
	mq := &TermFacet{}
	mq.data.Field = field
	mq.data.Size = size
	return mq
}

type numericFacetRange struct {
	Name  string  `json:"name,omitempty"`
	Start float64 `json:"start,omitempty"`
	End   float64 `json:"end,omitempty"`
}
type numericFacetData struct {
	Field         string              `json:"field,omitempty"`
	Size          int                 `json:"size,omitempty"`
	NumericRanges []numericFacetRange `json:"numeric_ranges,omitempty"`
}
type NumericFacet struct {
	data numericFacetData
}

func (f NumericFacet) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.data)
}

func (f NumericFacet) validate() {
	if len(f.data.NumericRanges) == 0 {
		panic(ErrFacetNoRanges)
	}
}

func (f *NumericFacet) AddRange(name string, start, end float64) *NumericFacet {
	f.data.NumericRanges = append(f.data.NumericRanges, numericFacetRange{
		Name:  name,
		Start: start,
		End:   end,
	})
	return f
}

func NewNumericFacet(field string, size int) *NumericFacet {
	mq := &NumericFacet{}
	mq.data.Field = field
	mq.data.Size = size
	return mq
}

type dateFacetRange struct {
	Name  string `json:"name,omitempty"`
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}
type dateFacetData struct {
	Field      string           `json:"field,omitempty"`
	Size       int              `json:"size,omitempty"`
	DateRanges []dateFacetRange `json:"date_ranges,omitempty"`
}
type DateFacet struct {
	data dateFacetData
}

func (f DateFacet) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.data)
}

func (f DateFacet) validate() {
	if len(f.data.DateRanges) == 0 {
		panic(ErrFacetNoRanges)
	}
}

func (f *DateFacet) AddRange(name string, start, end string) *DateFacet {
	f.data.DateRanges = append(f.data.DateRanges, dateFacetRange{
		Name:  name,
		Start: start,
		End:   end,
	})
	return f
}

func NewDateFacet(field string, size int) *DateFacet {
	mq := &DateFacet{}
	mq.data.Field = field
	mq.data.Size = size
	return mq
}
