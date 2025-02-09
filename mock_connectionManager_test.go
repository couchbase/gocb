// Code generated by mockery v2.46.2. DO NOT EDIT.

package gocb

import (
	gocbcore "github.com/couchbase/gocbcore/v10"
	mock "github.com/stretchr/testify/mock"
)

// mockConnectionManager is an autogenerated mock type for the connectionManager type
type mockConnectionManager struct {
	mock.Mock
}

// MarkOpBeginning provides a mock function with given fields:
func (_m *mockConnectionManager) MarkOpBeginning() {
	_m.Called()
}

// MarkOpCompleted provides a mock function with given fields:
func (_m *mockConnectionManager) MarkOpCompleted() {
	_m.Called()
}

// buildConfig provides a mock function with given fields: cluster
func (_m *mockConnectionManager) buildConfig(cluster *Cluster) error {
	ret := _m.Called(cluster)

	if len(ret) == 0 {
		panic("no return value specified for buildConfig")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*Cluster) error); ok {
		r0 = rf(cluster)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// close provides a mock function with given fields:
func (_m *mockConnectionManager) close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// connect provides a mock function with given fields:
func (_m *mockConnectionManager) connect() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for connect")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// connection provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) connection(bucketName string) (*gocbcore.Agent, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for connection")
	}

	var r0 *gocbcore.Agent
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*gocbcore.Agent, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) *gocbcore.Agent); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gocbcore.Agent)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getAnalyticsIndexProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getAnalyticsIndexProvider() (analyticsIndexProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getAnalyticsIndexProvider")
	}

	var r0 analyticsIndexProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (analyticsIndexProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() analyticsIndexProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(analyticsIndexProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getAnalyticsProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getAnalyticsProvider() (analyticsProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getAnalyticsProvider")
	}

	var r0 analyticsProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (analyticsProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() analyticsProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(analyticsProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getBucketManagementProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getBucketManagementProvider() (bucketManagementProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getBucketManagementProvider")
	}

	var r0 bucketManagementProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (bucketManagementProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() bucketManagementProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(bucketManagementProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getCollectionsManagementProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getCollectionsManagementProvider(bucketName string) (collectionsManagementProvider, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getCollectionsManagementProvider")
	}

	var r0 collectionsManagementProvider
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (collectionsManagementProvider, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) collectionsManagementProvider); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(collectionsManagementProvider)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getDiagnosticsProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getDiagnosticsProvider(bucketName string) (diagnosticsProvider, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getDiagnosticsProvider")
	}

	var r0 diagnosticsProvider
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (diagnosticsProvider, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) diagnosticsProvider); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(diagnosticsProvider)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getEventingManagementProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getEventingManagementProvider() (eventingManagementProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getEventingManagementProvider")
	}

	var r0 eventingManagementProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (eventingManagementProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() eventingManagementProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(eventingManagementProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getHTTPProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getHTTPProvider(bucketName string) (httpProvider, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getHTTPProvider")
	}

	var r0 httpProvider
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (httpProvider, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) httpProvider); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(httpProvider)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getInternalProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getInternalProvider() (internalProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getInternalProvider")
	}

	var r0 internalProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (internalProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() internalProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(internalProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getKvBulkProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getKvBulkProvider(bucketName string) (kvBulkProvider, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getKvBulkProvider")
	}

	var r0 kvBulkProvider
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (kvBulkProvider, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) kvBulkProvider); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(kvBulkProvider)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getKvCapabilitiesProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getKvCapabilitiesProvider(bucketName string) (kvCapabilityVerifier, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getKvCapabilitiesProvider")
	}

	var r0 kvCapabilityVerifier
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (kvCapabilityVerifier, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) kvCapabilityVerifier); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(kvCapabilityVerifier)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getKvProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getKvProvider(bucketName string) (kvProvider, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getKvProvider")
	}

	var r0 kvProvider
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (kvProvider, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) kvProvider); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(kvProvider)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getMeter provides a mock function with given fields:
func (_m *mockConnectionManager) getMeter() *meterWrapper {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getMeter")
	}

	var r0 *meterWrapper
	if rf, ok := ret.Get(0).(func() *meterWrapper); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*meterWrapper)
		}
	}

	return r0
}

// getQueryIndexProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getQueryIndexProvider() (queryIndexProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getQueryIndexProvider")
	}

	var r0 queryIndexProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (queryIndexProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() queryIndexProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(queryIndexProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getQueryProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getQueryProvider() (queryProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getQueryProvider")
	}

	var r0 queryProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (queryProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() queryProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(queryProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getSearchCapabilitiesProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getSearchCapabilitiesProvider() (searchCapabilityVerifier, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getSearchCapabilitiesProvider")
	}

	var r0 searchCapabilityVerifier
	var r1 error
	if rf, ok := ret.Get(0).(func() (searchCapabilityVerifier, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() searchCapabilityVerifier); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(searchCapabilityVerifier)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getSearchIndexProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getSearchIndexProvider() (searchIndexProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getSearchIndexProvider")
	}

	var r0 searchIndexProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (searchIndexProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() searchIndexProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(searchIndexProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getSearchProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getSearchProvider() (searchProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getSearchProvider")
	}

	var r0 searchProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (searchProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() searchProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(searchProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getTransactionsProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getTransactionsProvider() (transactionsProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getTransactionsProvider")
	}

	var r0 transactionsProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (transactionsProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() transactionsProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(transactionsProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getUserManagerProvider provides a mock function with given fields:
func (_m *mockConnectionManager) getUserManagerProvider() (userManagerProvider, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for getUserManagerProvider")
	}

	var r0 userManagerProvider
	var r1 error
	if rf, ok := ret.Get(0).(func() (userManagerProvider, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() userManagerProvider); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(userManagerProvider)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getViewIndexProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getViewIndexProvider(bucketName string) (viewIndexProvider, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getViewIndexProvider")
	}

	var r0 viewIndexProvider
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (viewIndexProvider, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) viewIndexProvider); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(viewIndexProvider)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getViewProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getViewProvider(bucketName string) (viewProvider, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getViewProvider")
	}

	var r0 viewProvider
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (viewProvider, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) viewProvider); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(viewProvider)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getWaitUntilReadyProvider provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) getWaitUntilReadyProvider(bucketName string) (waitUntilReadyProvider, error) {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for getWaitUntilReadyProvider")
	}

	var r0 waitUntilReadyProvider
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (waitUntilReadyProvider, error)); ok {
		return rf(bucketName)
	}
	if rf, ok := ret.Get(0).(func(string) waitUntilReadyProvider); ok {
		r0 = rf(bucketName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(waitUntilReadyProvider)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucketName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// initTransactions provides a mock function with given fields: config, cluster
func (_m *mockConnectionManager) initTransactions(config TransactionsConfig, cluster *Cluster) error {
	ret := _m.Called(config, cluster)

	if len(ret) == 0 {
		panic("no return value specified for initTransactions")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(TransactionsConfig, *Cluster) error); ok {
		r0 = rf(config, cluster)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// openBucket provides a mock function with given fields: bucketName
func (_m *mockConnectionManager) openBucket(bucketName string) error {
	ret := _m.Called(bucketName)

	if len(ret) == 0 {
		panic("no return value specified for openBucket")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(bucketName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// newMockConnectionManager creates a new instance of mockConnectionManager. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockConnectionManager(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockConnectionManager {
	mock := &mockConnectionManager{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
