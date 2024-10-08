// Code generated by mockery v2.46.2. DO NOT EDIT.

package gocb

import (
	context "context"

	gocbcore "github.com/couchbase/gocbcore/v10"
	mock "github.com/stretchr/testify/mock"
)

// mockViewProviderCoreProvider is an autogenerated mock type for the viewProviderCoreProvider type
type mockViewProviderCoreProvider struct {
	mock.Mock
}

// ViewQuery provides a mock function with given fields: ctx, opts
func (_m *mockViewProviderCoreProvider) ViewQuery(ctx context.Context, opts gocbcore.ViewQueryOptions) (viewRowReader, error) {
	ret := _m.Called(ctx, opts)

	if len(ret) == 0 {
		panic("no return value specified for ViewQuery")
	}

	var r0 viewRowReader
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, gocbcore.ViewQueryOptions) (viewRowReader, error)); ok {
		return rf(ctx, opts)
	}
	if rf, ok := ret.Get(0).(func(context.Context, gocbcore.ViewQueryOptions) viewRowReader); ok {
		r0 = rf(ctx, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(viewRowReader)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, gocbcore.ViewQueryOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newMockViewProviderCoreProvider creates a new instance of mockViewProviderCoreProvider. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockViewProviderCoreProvider(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockViewProviderCoreProvider {
	mock := &mockViewProviderCoreProvider{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
