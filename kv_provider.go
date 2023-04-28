package gocb

import gocbcore "github.com/couchbase/gocbcore/v10"

type kvProvider interface {
	Add(*kvOpManager) (*MutationResult, error)     // Done
	Set(*kvOpManager) (*MutationResult, error)     // Done
	Replace(*kvOpManager) (*MutationResult, error) // Done
	Get(*kvOpManager) (*GetResult, error)          // Done

	GetReplica(*kvOpManager) (*GetReplicaResult, error) // Done
	GetAllReplicas(*Collection, string, *GetAllReplicaOptions) (*GetAllReplicasResult, error)
	Exists(*kvOpManager) (*ExistsResult, error)   // Done
	Delete(*kvOpManager) (*MutationResult, error) //Done

	LookupIn(*kvOpManager, []LookupInSpec, SubdocDocFlag) (*LookupInResult, error)
	MutateIn(*kvOpManager, StoreSemantics, []MutateInSpec, SubdocDocFlag) (*MutateInResult, error)

	GetAndTouch(*kvOpManager) (*GetResult, error) // Done
	GetAndLock(*kvOpManager) (*GetResult, error)  // Done
	Unlock(*kvOpManager) error                    // Done
	Touch(*kvOpManager) (*MutationResult, error)  // Done

	Increment(*kvOpManager) (*CounterResult, error) //Done
	Decrement(*kvOpManager) (*CounterResult, error) //Done

	// Subdoc actions
	Append(*kvOpManager) (*MutationResult, error)  // Done
	Prepend(*kvOpManager) (*MutationResult, error) // Done
	Scan(c *Collection, scanType ScanType, opts *ScanOptions) (*ScanResult, error)

	//Bulk Actions
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
