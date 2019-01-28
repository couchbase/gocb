package gocb

type searchQueryData struct {
	Query interface{} `json:"query,omitempty"`
}

// SearchQuery represents a pending search query.
type SearchQuery struct {
	Name  string
	Query interface{}
}

func (sq *SearchQuery) indexName() string {
	return sq.Name
}

func (sq *SearchQuery) toSearchQueryData() (*searchQueryData, error) {
	return &searchQueryData{
		Query: sq.Query,
	}, nil
}
