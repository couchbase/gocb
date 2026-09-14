package gocb

import (
	"time"
)

type kvProvider interface {
	Insert(*Collection, string, interface{}, *InsertOptions) (*MutationResult, error)
	Upsert(*Collection, string, interface{}, *UpsertOptions) (*MutationResult, error)
	Replace(*Collection, string, interface{}, *ReplaceOptions) (*MutationResult, error)
	Remove(*Collection, string, *RemoveOptions) (*MutationResult, error)

	Get(*Collection, string, *GetOptions) (*GetResult, error)
	Exists(*Collection, string, *ExistsOptions) (*ExistsResult, error)
	GetAndTouch(*Collection, string, time.Duration, *GetAndTouchOptions) (*GetResult, error)
	GetAndLock(*Collection, string, time.Duration, *GetAndLockOptions) (*GetResult, error)
	Unlock(*Collection, string, Cas, *UnlockOptions) error
	Touch(*Collection, string, time.Duration, *TouchOptions) (*MutationResult, error)

	GetAnyReplica(*Collection, string, *GetAnyReplicaOptions) (*GetReplicaResult, error)
	GetAllReplicas(*Collection, string, *GetAllReplicaOptions) (*GetAllReplicasResult, error)
	GetReplica(*Collection, string, GetReplicaStrategy, *GetReplicaOptions) (*GetReplicaResult, error)

	LookupIn(*Collection, string, []LookupInSpec, *LookupInOptions) (*LookupInResult, error)
	LookupInAnyReplica(*Collection, string, []LookupInSpec, *LookupInAnyReplicaOptions) (*LookupInReplicaResult, error)
	LookupInAllReplicas(*Collection, string, []LookupInSpec, *LookupInAllReplicaOptions) (*LookupInAllReplicasResult, error)
	MutateIn(*Collection, string, []MutateInSpec, *MutateInOptions) (*MutateInResult, error)

	Increment(*Collection, string, *IncrementOptions) (*CounterResult, error)
	Decrement(*Collection, string, *DecrementOptions) (*CounterResult, error)
	Append(*Collection, string, []byte, *AppendOptions) (*MutationResult, error)
	Prepend(*Collection, string, []byte, *PrependOptions) (*MutationResult, error)

	Scan(*Collection, ScanType, *ScanOptions) (*ScanResult, error)

	StartKvOpTrace(*Collection, string, RequestSpan) *spanWrapper
}

type kvBulkProvider interface {
	Do(*Collection, []BulkOp, *BulkOpOptions) error
}
