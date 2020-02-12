package gocb

import "github.com/stretchr/testify/mock"

type mockPendingOp struct {
	mock.Mock
}

func (_m *mockPendingOp) Cancel(err error) {
	_m.Called(err)
}
