// Code generated by mockery v2.46.2. DO NOT EDIT.

package gocb

import (
	context "context"
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// mockWaitUntilReadyProvider is an autogenerated mock type for the waitUntilReadyProvider type
type mockWaitUntilReadyProvider struct {
	mock.Mock
}

// WaitUntilReady provides a mock function with given fields: ctx, deadline, opts
func (_m *mockWaitUntilReadyProvider) WaitUntilReady(ctx context.Context, deadline time.Time, opts *WaitUntilReadyOptions) error {
	ret := _m.Called(ctx, deadline, opts)

	if len(ret) == 0 {
		panic("no return value specified for WaitUntilReady")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, time.Time, *WaitUntilReadyOptions) error); ok {
		r0 = rf(ctx, deadline, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// newMockWaitUntilReadyProvider creates a new instance of mockWaitUntilReadyProvider. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockWaitUntilReadyProvider(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockWaitUntilReadyProvider {
	mock := &mockWaitUntilReadyProvider{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
