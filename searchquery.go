package gocb

import (
	"time"
)

type SearchHighlightStyle string

const (
	HtmlHighlightStyle  = SearchHighlightStyle("html")
	AnsiHightlightStyle = SearchHighlightStyle("ansi")
)

type searchQueryHighlightData struct {
	Style  string   `json:"style,omitempty"`
	Fields []string `json:"fields,omitempty"`
}
type searchQueryCtlData struct {
	Timeout uint `json:"timeout,omitempty"`
}
type searchQueryData struct {
	Query     FtsQuery                  `json:"query,omitempty"`
	Size      int                       `json:"size,omitempty"`
	From      int                       `json:"from,omitempty"`
	Explain   bool                      `json:"explain,omitempty"`
	Highlight *searchQueryHighlightData `json:"highlight,omitempty"`
	Fields    []string                  `json:"fields,omitempty"`
	Facets    map[string]FtsFacet       `json:"facets,omitempty"`
	Ctl       *searchQueryCtlData       `json:"ctl,omitempty"`
}

type SearchQuery struct {
	name string
	data searchQueryData
}

func (sq *SearchQuery) Limit(value int) *SearchQuery {
	sq.data.Size = value
	return sq
}

func (sq *SearchQuery) Skip(value int) *SearchQuery {
	sq.data.From = value
	return sq
}

func (sq *SearchQuery) Explain(value bool) *SearchQuery {
	sq.data.Explain = value
	return sq
}

func (sq *SearchQuery) Highlight(style SearchHighlightStyle, fields ...string) *SearchQuery {
	if sq.data.Highlight == nil {
		sq.data.Highlight = &searchQueryHighlightData{}
	}
	sq.data.Highlight.Style = string(style)
	sq.data.Highlight.Fields = fields
	return sq
}

func (sq *SearchQuery) Fields(fields ...string) *SearchQuery {
	sq.data.Fields = fields
	return sq
}

func (sq *SearchQuery) AddFacet(name string, facet FtsFacet) *SearchQuery {
	facet.validate()
	if sq.data.Facets == nil {
		sq.data.Facets = make(map[string]FtsFacet)
	}
	sq.data.Facets[name] = facet
	return sq
}

func (sq *SearchQuery) Timeout(value time.Duration) *SearchQuery {
	if sq.data.Ctl == nil {
		sq.data.Ctl = &searchQueryCtlData{}
	}
	sq.data.Ctl.Timeout = uint(value / time.Millisecond)
	return sq
}

func (sq *SearchQuery) indexName() string {
	return sq.name
}

func (sq *SearchQuery) queryData() interface{} {
	return sq.data
}

func NewSearchQuery(indexName string, query FtsQuery) *SearchQuery {
	q := &SearchQuery{
		name: indexName,
	}
	q.data.Query = query
	return q
}
