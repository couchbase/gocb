package gocb

import (
	gocbcore "github.com/couchbase/gocbcore/v10"
	"time"
)

type connectionManager interface {
	connect() error
	openBucket(bucketName string) error
	buildConfig(cluster *Cluster) error
	connection(bucketName string) (*gocbcore.Agent, error)
	close() error

	getKvProvider(bucketName string) (kvProvider, error)
	getKvBulkProvider(bucketName string) (kvBulkProvider, error)
	getKvCapabilitiesProvider(bucketName string) (kvCapabilityVerifier, error)
	getViewProvider(bucketName string) (viewProvider, error)
	getViewIndexProvider(bucketName string) (viewIndexProvider, error)
	getQueryProvider() (queryProvider, error)
	getQueryIndexProvider() (queryIndexProvider, error)
	getAnalyticsProvider() (analyticsProvider, error)
	getAnalyticsIndexProvider() (analyticsIndexProvider, error)
	getSearchProvider() (searchProvider, error)
	getHTTPProvider(bucketName string) (httpProvider, error)
	getDiagnosticsProvider(bucketName string) (diagnosticsProvider, error)
	getWaitUntilReadyProvider(bucketName string) (waitUntilReadyProvider, error)
	getCollectionsManagementProvider(bucketName string) (collectionsManagementProvider, error)
	getBucketManagementProvider() (bucketManagementProvider, error)
	getSearchIndexProvider() (searchIndexProvider, error)
	getSearchCapabilitiesProvider() (searchCapabilityVerifier, error)
	getEventingManagementProvider() (eventingManagementProvider, error)
	getUserManagerProvider() (userManagerProvider, error)
	getInternalProvider() (internalProvider, error)

	initTransactions(config TransactionsConfig, cluster *Cluster) error
	getTransactionsProvider() (transactionsProvider, error)

	getMeter() *meterWrapper

	opController
}

type opController interface {
	MarkOpBeginning()
	MarkOpCompleted()
}

type providerController[P any] struct {
	get func() (P, error)
	opController

	// Metrics-related fields
	meter    *meterWrapper
	keyspace *keyspace
	service  string
}

func autoOpControl[T any, P any](controller *providerController[P], operation string, opFn func(P) (T, error)) (T, error) {
	controller.MarkOpBeginning()
	defer controller.MarkOpCompleted()

	p, err := controller.get()
	if err != nil {
		var emptyT T
		return emptyT, err
	}

	start := time.Now()
	retT, err := opFn(p)

	if operation != "" && controller.meter != nil {
		defer controller.meter.ValueRecord(controller.service, operation, start, controller.keyspace, err)
	}

	if err != nil {
		var emptyT T
		return emptyT, err
	}

	return retT, nil
}

func autoOpControlErrorOnly[P any](controller *providerController[P], operation string, opFn func(P) error) error {
	_, err := autoOpControl(controller, operation, func(provider P) (struct{}, error) {
		err := opFn(provider)
		return struct{}{}, err
	})

	return err
}

type newConnectionMgrOptions struct {
	tracer *tracerWrapper
	meter  *meterWrapper

	preferredServerGroup string
}

func (c *Cluster) newConnectionMgr(protocol string, opts *newConnectionMgrOptions) connectionManager {
	switch protocol {
	case "couchbase2":
		return &psConnectionMgr{
			timeouts:     c.timeoutsConfig,
			tracer:       opts.tracer,
			meter:        opts.meter,
			defaultRetry: c.retryStrategyWrapper.wrapped,
		}
	default:
		return &stdConnectionMgr{
			retryStrategyWrapper: c.retryStrategyWrapper,
			transcoder:           c.transcoder,
			timeouts:             c.timeoutsConfig,
			tracer:               opts.tracer,
			meter:                opts.meter,
			preferredServerGroup: opts.preferredServerGroup,
		}
	}
}
