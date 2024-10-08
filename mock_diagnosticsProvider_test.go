// Code generated by mockery v2.46.2. DO NOT EDIT.

package gocb

import mock "github.com/stretchr/testify/mock"

// mockDiagnosticsProvider is an autogenerated mock type for the diagnosticsProvider type
type mockDiagnosticsProvider struct {
	mock.Mock
}

// Diagnostics provides a mock function with given fields: opts
func (_m *mockDiagnosticsProvider) Diagnostics(opts *DiagnosticsOptions) (*DiagnosticsResult, error) {
	ret := _m.Called(opts)

	if len(ret) == 0 {
		panic("no return value specified for Diagnostics")
	}

	var r0 *DiagnosticsResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*DiagnosticsOptions) (*DiagnosticsResult, error)); ok {
		return rf(opts)
	}
	if rf, ok := ret.Get(0).(func(*DiagnosticsOptions) *DiagnosticsResult); ok {
		r0 = rf(opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*DiagnosticsResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*DiagnosticsOptions) error); ok {
		r1 = rf(opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Ping provides a mock function with given fields: opts
func (_m *mockDiagnosticsProvider) Ping(opts *PingOptions) (*PingResult, error) {
	ret := _m.Called(opts)

	if len(ret) == 0 {
		panic("no return value specified for Ping")
	}

	var r0 *PingResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*PingOptions) (*PingResult, error)); ok {
		return rf(opts)
	}
	if rf, ok := ret.Get(0).(func(*PingOptions) *PingResult); ok {
		r0 = rf(opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*PingResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*PingOptions) error); ok {
		r1 = rf(opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newMockDiagnosticsProvider creates a new instance of mockDiagnosticsProvider. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDiagnosticsProvider(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDiagnosticsProvider {
	mock := &mockDiagnosticsProvider{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
