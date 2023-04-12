package gocb

import gocbcore "github.com/couchbase/gocbcore/v10"

type kvProvider interface {
	Add(*kvOpManager) (*MutationResult, error)
	Set(*kvOpManager) (*MutationResult, error)
	Replace(*kvOpManager) (*MutationResult, error)
	Get(*kvOpManager) (*GetResult, error)
	//GetOneReplica(opts GetOptions) (*GetResult, error)
	Exists(*kvOpManager) (*ExistsResult, error)

	Delete(opts RemoveOptions) (*MutationResult, error)
	LookupIn(opts LookupInResult) (*LookupInResult, error)
	MutateIn(opts MutateInOptions) (*MutateInResult, error)

	GetAndTouch(opts GetAndTouchOptions) (*GetResult, error)
	GetAndLock(opts GetAndLockOptions) (*GetResult, error)
	Unlock(opts gocbcore.UnlockOptions) error
	Touch(opts gocbcore.TouchOptions) (*MutationResult, error)
	Increment(opts IncrementOptions) (*CounterResult, error)
	Decrement(opts DecrementOptions) (*CounterResult, error)

	Append(opts AppendOptions) (*MutationResult, error)
	Prepend(opts PrependOptions) (*MutationResult, error)
	Scan(scanType ScanType, opts ScanOptions) (*ScanResult, error)
}
