package gocb

import (
	"time"

	gocbcore "github.com/couchbase/gocbcore/v10"
)

type kvProvider interface {
	Insert(*Collection, string, interface{}, *InsertOptions) (*MutationResult, error)   // Done
	Upsert(*Collection, string, interface{}, *UpsertOptions) (*MutationResult, error)   // Done
	Replace(*Collection, string, interface{}, *ReplaceOptions) (*MutationResult, error) // Done
	Remove(*Collection, string, *RemoveOptions) (*MutationResult, error)                // Done

	Get(*Collection, string, *GetOptions) (*GetResult, error)                                // Done
	Exists(*Collection, string, *ExistsOptions) (*ExistsResult, error)                       // Done
	GetAndTouch(*Collection, string, time.Duration, *GetAndTouchOptions) (*GetResult, error) // Done
	GetAndLock(*Collection, string, time.Duration, *GetAndLockOptions) (*GetResult, error)   // Done
	Unlock(*Collection, string, Cas, *UnlockOptions) error                                   // Done
	Touch(*Collection, string, time.Duration, *TouchOptions) (*MutationResult, error)        // Done

	GetAnyReplica(c *Collection, id string, opts *GetAnyReplicaOptions) (*GetReplicaResult, error)
	GetAllReplicas(*Collection, string, *GetAllReplicaOptions) (*GetAllReplicasResult, error)

	LookupIn(*Collection, string, []LookupInSpec, *LookupInOptions) (*LookupInResult, error)
	MutateIn(*Collection, string, []MutateInSpec, *MutateInOptions) (*MutateInResult, error)

	Increment(*Collection, string, *IncrementOptions) (*CounterResult, error)      // Done
	Decrement(*Collection, string, *DecrementOptions) (*CounterResult, error)      // Done
	Append(*Collection, string, []byte, *AppendOptions) (*MutationResult, error)   // Done
	Prepend(*Collection, string, []byte, *PrependOptions) (*MutationResult, error) // Done

	Scan(*Collection, ScanType, *ScanOptions) (*ScanResult, error)

	BulkGet(gocbcore.GetOptions, gocbcore.GetCallback) (gocbcore.PendingOp, error)
	BulkGetAndTouch(gocbcore.GetAndTouchOptions, gocbcore.GetAndTouchCallback) (gocbcore.PendingOp, error)
	BulkTouch(gocbcore.TouchOptions, gocbcore.TouchCallback) (gocbcore.PendingOp, error)
	BulkDelete(gocbcore.DeleteOptions, gocbcore.DeleteCallback) (gocbcore.PendingOp, error)
	BulkSet(gocbcore.SetOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error)
	BulkAdd(gocbcore.AddOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error)
	BulkReplace(gocbcore.ReplaceOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error)
	BulkAppend(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) (gocbcore.PendingOp, error)
	BulkPrepend(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) (gocbcore.PendingOp, error)
	BulkIncrement(gocbcore.CounterOptions, gocbcore.CounterCallback) (gocbcore.PendingOp, error)
	BulkDecrement(gocbcore.CounterOptions, gocbcore.CounterCallback) (gocbcore.PendingOp, error)
}
