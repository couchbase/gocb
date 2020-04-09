package gocb

import "github.com/stretchr/testify/mock"

type mockPendingOp struct {
	mock.Mock
}

func (_m *mockPendingOp) Cancel() {
	_m.Called()
}
