package gocb

// AnalyticsQuery executes the analytics query statement on the server, constraining the query to the bucket and scope.
func (s *Scope) AnalyticsQuery(statement string, opts *AnalyticsOptions) (*AnalyticsResult, error) {
	if opts == nil {
		opts = &AnalyticsOptions{}
	}

	provider, err := s.getAnalyticsProvider()
	if err != nil {
		return nil, maybeEnhanceAnalyticsError(err)
	}

	return provider.AnalyticsQuery(statement, s, opts)
}
