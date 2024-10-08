// Code generated by mockery v2.46.2. DO NOT EDIT.

package gocb

import (
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// mockKvProvider is an autogenerated mock type for the kvProvider type
type mockKvProvider struct {
	mock.Mock
}

// Append provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) Append(_a0 *Collection, _a1 string, _a2 []byte, _a3 *AppendOptions) (*MutationResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for Append")
	}

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, []byte, *AppendOptions) (*MutationResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, []byte, *AppendOptions) *MutationResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, []byte, *AppendOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Decrement provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) Decrement(_a0 *Collection, _a1 string, _a2 *DecrementOptions) (*CounterResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for Decrement")
	}

	var r0 *CounterResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, *DecrementOptions) (*CounterResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, *DecrementOptions) *CounterResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*CounterResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, *DecrementOptions) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Exists provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) Exists(_a0 *Collection, _a1 string, _a2 *ExistsOptions) (*ExistsResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for Exists")
	}

	var r0 *ExistsResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, *ExistsOptions) (*ExistsResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, *ExistsOptions) *ExistsResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ExistsResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, *ExistsOptions) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Get provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) Get(_a0 *Collection, _a1 string, _a2 *GetOptions) (*GetResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 *GetResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, *GetOptions) (*GetResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, *GetOptions) *GetResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, *GetOptions) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllReplicas provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) GetAllReplicas(_a0 *Collection, _a1 string, _a2 *GetAllReplicaOptions) (*GetAllReplicasResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for GetAllReplicas")
	}

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

// GetAndLock provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) GetAndLock(_a0 *Collection, _a1 string, _a2 time.Duration, _a3 *GetAndLockOptions) (*GetResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for GetAndLock")
	}

	var r0 *GetResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, time.Duration, *GetAndLockOptions) (*GetResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, time.Duration, *GetAndLockOptions) *GetResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, time.Duration, *GetAndLockOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAndTouch provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) GetAndTouch(_a0 *Collection, _a1 string, _a2 time.Duration, _a3 *GetAndTouchOptions) (*GetResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for GetAndTouch")
	}

	var r0 *GetResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, time.Duration, *GetAndTouchOptions) (*GetResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, time.Duration, *GetAndTouchOptions) *GetResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, time.Duration, *GetAndTouchOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAnyReplica provides a mock function with given fields: c, id, opts
func (_m *mockKvProvider) GetAnyReplica(c *Collection, id string, opts *GetAnyReplicaOptions) (*GetReplicaResult, error) {
	ret := _m.Called(c, id, opts)

	if len(ret) == 0 {
		panic("no return value specified for GetAnyReplica")
	}

	var r0 *GetReplicaResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, *GetAnyReplicaOptions) (*GetReplicaResult, error)); ok {
		return rf(c, id, opts)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, *GetAnyReplicaOptions) *GetReplicaResult); ok {
		r0 = rf(c, id, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetReplicaResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, *GetAnyReplicaOptions) error); ok {
		r1 = rf(c, id, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Increment provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) Increment(_a0 *Collection, _a1 string, _a2 *IncrementOptions) (*CounterResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for Increment")
	}

	var r0 *CounterResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, *IncrementOptions) (*CounterResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, *IncrementOptions) *CounterResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*CounterResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, *IncrementOptions) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Insert provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) Insert(_a0 *Collection, _a1 string, _a2 interface{}, _a3 *InsertOptions) (*MutationResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for Insert")
	}

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, interface{}, *InsertOptions) (*MutationResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, interface{}, *InsertOptions) *MutationResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, interface{}, *InsertOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LookupIn provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) LookupIn(_a0 *Collection, _a1 string, _a2 []LookupInSpec, _a3 *LookupInOptions) (*LookupInResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for LookupIn")
	}

	var r0 *LookupInResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, []LookupInSpec, *LookupInOptions) (*LookupInResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, []LookupInSpec, *LookupInOptions) *LookupInResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*LookupInResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, []LookupInSpec, *LookupInOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LookupInAllReplicas provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) LookupInAllReplicas(_a0 *Collection, _a1 string, _a2 []LookupInSpec, _a3 *LookupInAllReplicaOptions) (*LookupInAllReplicasResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for LookupInAllReplicas")
	}

	var r0 *LookupInAllReplicasResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, []LookupInSpec, *LookupInAllReplicaOptions) (*LookupInAllReplicasResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, []LookupInSpec, *LookupInAllReplicaOptions) *LookupInAllReplicasResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*LookupInAllReplicasResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, []LookupInSpec, *LookupInAllReplicaOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LookupInAnyReplica provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) LookupInAnyReplica(_a0 *Collection, _a1 string, _a2 []LookupInSpec, _a3 *LookupInAnyReplicaOptions) (*LookupInReplicaResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for LookupInAnyReplica")
	}

	var r0 *LookupInReplicaResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, []LookupInSpec, *LookupInAnyReplicaOptions) (*LookupInReplicaResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, []LookupInSpec, *LookupInAnyReplicaOptions) *LookupInReplicaResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*LookupInReplicaResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, []LookupInSpec, *LookupInAnyReplicaOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MutateIn provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) MutateIn(_a0 *Collection, _a1 string, _a2 []MutateInSpec, _a3 *MutateInOptions) (*MutateInResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for MutateIn")
	}

	var r0 *MutateInResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, []MutateInSpec, *MutateInOptions) (*MutateInResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, []MutateInSpec, *MutateInOptions) *MutateInResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutateInResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, []MutateInSpec, *MutateInOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Prepend provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) Prepend(_a0 *Collection, _a1 string, _a2 []byte, _a3 *PrependOptions) (*MutationResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for Prepend")
	}

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, []byte, *PrependOptions) (*MutationResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, []byte, *PrependOptions) *MutationResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, []byte, *PrependOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Remove provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) Remove(_a0 *Collection, _a1 string, _a2 *RemoveOptions) (*MutationResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for Remove")
	}

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, *RemoveOptions) (*MutationResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, *RemoveOptions) *MutationResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, *RemoveOptions) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Replace provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) Replace(_a0 *Collection, _a1 string, _a2 interface{}, _a3 *ReplaceOptions) (*MutationResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for Replace")
	}

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, interface{}, *ReplaceOptions) (*MutationResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, interface{}, *ReplaceOptions) *MutationResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, interface{}, *ReplaceOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Scan provides a mock function with given fields: _a0, _a1, _a2
func (_m *mockKvProvider) Scan(_a0 *Collection, _a1 ScanType, _a2 *ScanOptions) (*ScanResult, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for Scan")
	}

	var r0 *ScanResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, ScanType, *ScanOptions) (*ScanResult, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(*Collection, ScanType, *ScanOptions) *ScanResult); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ScanResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, ScanType, *ScanOptions) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// StartKvOpTrace provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) StartKvOpTrace(_a0 *Collection, _a1 string, _a2 RequestSpan, _a3 bool) RequestSpan {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for StartKvOpTrace")
	}

	var r0 RequestSpan
	if rf, ok := ret.Get(0).(func(*Collection, string, RequestSpan, bool) RequestSpan); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(RequestSpan)
		}
	}

	return r0
}

// Touch provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) Touch(_a0 *Collection, _a1 string, _a2 time.Duration, _a3 *TouchOptions) (*MutationResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for Touch")
	}

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, time.Duration, *TouchOptions) (*MutationResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, time.Duration, *TouchOptions) *MutationResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, time.Duration, *TouchOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Unlock provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) Unlock(_a0 *Collection, _a1 string, _a2 Cas, _a3 *UnlockOptions) error {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for Unlock")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*Collection, string, Cas, *UnlockOptions) error); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Upsert provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *mockKvProvider) Upsert(_a0 *Collection, _a1 string, _a2 interface{}, _a3 *UpsertOptions) (*MutationResult, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for Upsert")
	}

	var r0 *MutationResult
	var r1 error
	if rf, ok := ret.Get(0).(func(*Collection, string, interface{}, *UpsertOptions) (*MutationResult, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(*Collection, string, interface{}, *UpsertOptions) *MutationResult); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*MutationResult)
		}
	}

	if rf, ok := ret.Get(1).(func(*Collection, string, interface{}, *UpsertOptions) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newMockKvProvider creates a new instance of mockKvProvider. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockKvProvider(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockKvProvider {
	mock := &mockKvProvider{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
