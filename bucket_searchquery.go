package gocb

// ExecuteSearchQuery performs a view query and returns a list of rows or an error.
func (b *Bucket) ExecuteSearchQuery(q *SearchQuery) (SearchResults, error) {
	return b.cluster.doSearchQuery(b, q)
}
