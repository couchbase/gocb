package gocb

// ExecuteN1qlQuery performs a n1ql query and returns a list of rows or an error.
func (b *Bucket) ExecuteN1qlQuery(q *N1qlQuery, params interface{}) (QueryResults, error) {
	return b.cluster.doN1qlQuery(b, q, params)
}
