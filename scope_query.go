package gocb

// Query executes the query statement on the server, constraining the query to the bucket and scope.
func (s *Scope) Query(statement string, opts *QueryOptions) (*QueryResult, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}

	if opts.AsTransaction != nil {
		return s.getTransactions().singleQuery(statement, s, *opts)
	}

	provider, err := s.getQueryProvider()
	if err != nil {
		return nil, QueryError{
			InnerError:      wrapError(err, "failed to get query provider"),
			Statement:       statement,
			ClientContextID: opts.ClientContextID,
		}
	}

	return provider.Query(statement, s, opts)
}
