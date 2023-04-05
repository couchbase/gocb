// Code generated by mockery v2.26.1. DO NOT EDIT.

package gocb

import (
	gocbcore "github.com/couchbase/gocbcore/v10"
	mock "github.com/stretchr/testify/mock"
)

// mockKvProvider is an autogenerated mock type for the kvProvider type
type mockKvProvider struct {
	mock.Mock
}

// Add provides a mock function with given fields: _a0
func (_m *mockKvProvider) Add(_a0 *kvOpManagerCore) (*MutationResult, error) {
	ret := _m.Called(_a0)

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*MutationResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *MutationResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Append provides a mock function with given fields: _a0
func (_m *mockKvProvider) Append(_a0 *kvOpManagerCore) (*MutationResult, error) {
	ret := _m.Called(_a0)

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*MutationResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *MutationResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkAdd provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkAdd(_a0 gocbcore.AddOptions, _a1 gocbcore.StoreCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.AddOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.AddOptions, gocbcore.StoreCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.AddOptions, gocbcore.StoreCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkAppend provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkAppend(_a0 gocbcore.AdjoinOptions, _a1 gocbcore.AdjoinCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkDecrement provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkDecrement(_a0 gocbcore.CounterOptions, _a1 gocbcore.CounterCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.CounterOptions, gocbcore.CounterCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.CounterOptions, gocbcore.CounterCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.CounterOptions, gocbcore.CounterCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkDelete provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkDelete(_a0 gocbcore.DeleteOptions, _a1 gocbcore.DeleteCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.DeleteOptions, gocbcore.DeleteCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.DeleteOptions, gocbcore.DeleteCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.DeleteOptions, gocbcore.DeleteCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkGet provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkGet(_a0 gocbcore.GetOptions, _a1 gocbcore.GetCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.GetOptions, gocbcore.GetCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.GetOptions, gocbcore.GetCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.GetOptions, gocbcore.GetCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkGetAndTouch provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkGetAndTouch(_a0 gocbcore.GetAndTouchOptions, _a1 gocbcore.GetAndTouchCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.GetAndTouchOptions, gocbcore.GetAndTouchCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.GetAndTouchOptions, gocbcore.GetAndTouchCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.GetAndTouchOptions, gocbcore.GetAndTouchCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkIncrement provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkIncrement(_a0 gocbcore.CounterOptions, _a1 gocbcore.CounterCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.CounterOptions, gocbcore.CounterCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.CounterOptions, gocbcore.CounterCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.CounterOptions, gocbcore.CounterCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkPrepend provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkPrepend(_a0 gocbcore.AdjoinOptions, _a1 gocbcore.AdjoinCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkReplace provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkReplace(_a0 gocbcore.ReplaceOptions, _a1 gocbcore.StoreCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.ReplaceOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.ReplaceOptions, gocbcore.StoreCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.ReplaceOptions, gocbcore.StoreCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkSet provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkSet(_a0 gocbcore.SetOptions, _a1 gocbcore.StoreCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.SetOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.SetOptions, gocbcore.StoreCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.SetOptions, gocbcore.StoreCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// BulkTouch provides a mock function with given fields: _a0, _a1
func (_m *mockKvProvider) BulkTouch(_a0 gocbcore.TouchOptions, _a1 gocbcore.TouchCallback) (gocbcore.PendingOp, error) {
	ret := _m.Called(_a0, _a1)

	var r0 gocbcore.PendingOp
	var r1 error
	if rf, ok := ret.Get(0).(func(gocbcore.TouchOptions, gocbcore.TouchCallback) (gocbcore.PendingOp, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(gocbcore.TouchOptions, gocbcore.TouchCallback) gocbcore.PendingOp); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gocbcore.PendingOp)
		}
	}

	if rf, ok := ret.Get(1).(func(gocbcore.TouchOptions, gocbcore.TouchCallback) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Decrement provides a mock function with given fields: _a0
func (_m *mockKvProvider) Decrement(_a0 *kvOpManagerCore) (*CounterResult, error) {
	ret := _m.Called(_a0)

	var r0 *CounterResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*CounterResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *CounterResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*CounterResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Delete provides a mock function with given fields: _a0
func (_m *mockKvProvider) Delete(_a0 *kvOpManagerCore) (*MutationResult, error) {
	ret := _m.Called(_a0)

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*MutationResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *MutationResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Exists provides a mock function with given fields: _a0
func (_m *mockKvProvider) Exists(_a0 *kvOpManagerCore) (*ExistsResult, error) {
	ret := _m.Called(_a0)

	var r0 *ExistsResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*ExistsResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *ExistsResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ExistsResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Get provides a mock function with given fields: _a0
func (_m *mockKvProvider) Get(_a0 *kvOpManagerCore) (*GetResult, error) {
	ret := _m.Called(_a0)

	var r0 *GetResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*GetResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *GetResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllReplicas provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) GetAllReplicas(_a0 *Collection, _a1 string, _a2 *GetAllReplicaOptions) (*GetAllReplicasResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *GetAllReplicasResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, *GetAllReplicaOptions) (*GetAllReplicasResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, *GetAllReplicaOptions) *GetAllReplicasResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetAllReplicasResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, *GetAllReplicaOptions) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAndLock provides a mock function with given fields: _a0
func (_m *mockKvProvider) GetAndLock(_a0 *kvOpManagerCore) (*GetResult, error) {
	ret := _m.Called(_a0)

	var r0 *GetResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*GetResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *GetResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAndTouch provides a mock function with given fields: _a0
func (_m *mockKvProvider) GetAndTouch(_a0 *kvOpManagerCore) (*GetResult, error) {
	ret := _m.Called(_a0)

	var r0 *GetResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*GetResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *GetResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetReplica provides a mock function with given fields: _a0
func (_m *mockKvProvider) GetReplica(_a0 *kvOpManagerCore) (*GetReplicaResult, error) {
	ret := _m.Called(_a0)

	var r0 *GetReplicaResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*GetReplicaResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *GetReplicaResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetReplicaResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Increment provides a mock function with given fields: _a0
func (_m *mockKvProvider) Increment(_a0 *kvOpManagerCore) (*CounterResult, error) {
	ret := _m.Called(_a0)

	var r0 *CounterResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*CounterResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *CounterResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*CounterResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LookupIn provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) LookupIn(_a0 *kvOpManagerCore, _a1 []LookupInSpec, _a2 SubdocDocFlag) (*LookupInResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *LookupInResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore, []LookupInSpec, SubdocDocFlag) (*LookupInResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore, []LookupInSpec, SubdocDocFlag) *LookupInResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*LookupInResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore, []LookupInSpec, SubdocDocFlag) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MutateIn provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) MutateIn(_a0 *kvOpManagerCore, _a1 StoreSemantics, _a2 []MutateInSpec, _a3 SubdocDocFlag) (*MutateInResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	var r0 *MutateInResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore, StoreSemantics, []MutateInSpec, SubdocDocFlag) (*MutateInResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore, StoreSemantics, []MutateInSpec, SubdocDocFlag) *MutateInResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutateInResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore, StoreSemantics, []MutateInSpec, SubdocDocFlag) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Prepend provides a mock function with given fields: _a0
func (_m *mockKvProvider) Prepend(_a0 *kvOpManagerCore) (*MutationResult, error) {
	ret := _m.Called(_a0)

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*MutationResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *MutationResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Replace provides a mock function with given fields: _a0
func (_m *mockKvProvider) Replace(_a0 *kvOpManagerCore) (*MutationResult, error) {
	ret := _m.Called(_a0)

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*MutationResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *MutationResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Scan provides a mock function with given fields: c, scanType, opts
func (_m *mockKvProvider) Scan(c *Collection, scanType ScanType, opts *ScanOptions) (*ScanResult, error) {
	ret := _m.Called(c, scanType, opts)

	var r0 *ScanResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, ScanType, *ScanOptions) (*ScanResult, error)); ok {
		return rf(c, scanType, opts)
	}
	if rf, ok := ret.Get(0).(func(*Collection, ScanType, *ScanOptions) *ScanResult); ok {
		r0 = rf(c, scanType, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ScanResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, ScanType, *ScanOptions) error); ok {
		r1 = rf(c, scanType, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Set provides a mock function with given fields: _a0
func (_m *mockKvProvider) Set(_a0 *kvOpManagerCore) (*MutationResult, error) {
	ret := _m.Called(_a0)

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*MutationResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *MutationResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Touch provides a mock function with given fields: _a0
func (_m *mockKvProvider) Touch(_a0 *kvOpManagerCore) (*MutationResult, error) {
	ret := _m.Called(_a0)

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) (*MutationResult, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) *MutationResult); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*kvOpManagerCore) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Unlock provides a mock function with given fields: _a0
func (_m *mockKvProvider) Unlock(_a0 *kvOpManagerCore) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*kvOpManagerCore) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTnewMockKvProvider interface {
	mock.TestingT
	Cleanup(func())
}

// newMockKvProvider creates a new instance of mockKvProvider. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func newMockKvProvider(t mockConstructorTestingTnewMockKvProvider) *mockKvProvider {
	mock := &mockKvProvider{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
