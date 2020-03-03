package gocb

import (
	gocbcore "github.com/couchbase/gocbcore/v8"
)

type httpProvider interface {
	DoHTTPRequest(req *gocbcore.HTTPRequest) (*gocbcore.HTTPResponse, error)
}

type viewProvider interface {
	ViewQuery(opts gocbcore.ViewQueryOptions) (viewRowReader, error)
}

type queryProvider interface {
	N1QLQuery(opts gocbcore.N1QLQueryOptions) (queryRowReader, error)
}

type analyticsProvider interface {
	AnalyticsQuery(opts gocbcore.AnalyticsQueryOptions) (analyticsRowReader, error)
}

type searchProvider interface {
	SearchQuery(opts gocbcore.SearchQueryOptions) (searchRowReader, error)
}

type clusterCapabilityProvider interface {
	SupportsClusterCapability(capability gocbcore.ClusterCapability) bool
}

type diagnosticsProvider interface {
	Diagnostics() (*gocbcore.DiagnosticInfo, error)
}

type analyticsProviderWrapper struct {
	provider *gocbcore.Agent
}

func (apw *analyticsProviderWrapper) AnalyticsQuery(opts gocbcore.AnalyticsQueryOptions) (analyticsRowReader, error) {
	return apw.provider.AnalyticsQuery(opts)
}

type queryProviderWrapper struct {
	provider *gocbcore.Agent
}

func (apw *queryProviderWrapper) N1QLQuery(opts gocbcore.N1QLQueryOptions) (queryRowReader, error) {
	return apw.provider.N1QLQuery(opts)
}

type searchProviderWrapper struct {
	provider *gocbcore.Agent
}

func (apw *searchProviderWrapper) SearchQuery(opts gocbcore.SearchQueryOptions) (searchRowReader, error) {
	return apw.provider.SearchQuery(opts)
}

type viewProviderWrapper struct {
	provider *gocbcore.Agent
}

func (apw *viewProviderWrapper) ViewQuery(opts gocbcore.ViewQueryOptions) (viewRowReader, error) {
	return apw.provider.ViewQuery(opts)
}
