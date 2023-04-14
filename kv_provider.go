package gocb

import gocbcore "github.com/couchbase/gocbcore/v10"

type kvProvider interface {
	Add(*kvOpManager) (*MutationResult, error)     // Done
	Set(*kvOpManager) (*MutationResult, error)     // Done
	Replace(*kvOpManager) (*MutationResult, error) // Done
	Get(*kvOpManager) (*GetResult, error)          // Done
	//GetOneReplica(opts GetOptions) (*GetResult, error)
	Exists(*kvOpManager) (*ExistsResult, error)   // Done
	Delete(*kvOpManager) (*MutationResult, error) //Done

	LookupIn(opts LookupInResult) (*LookupInResult, error)
	MutateIn(opts MutateInOptions) (*MutateInResult, error)

	GetAndTouch(*kvOpManager) (*GetResult, error) // Done
	GetAndLock(*kvOpManager) (*GetResult, error)  // Done
	Unlock(*kvOpManager) error
	Touch(opts gocbcore.TouchOptions) (*MutationResult, error)
	Increment(opts IncrementOptions) (*CounterResult, error)
	Decrement(opts DecrementOptions) (*CounterResult, error)

	Append(opts AppendOptions) (*MutationResult, error)
	Prepend(opts PrependOptions) (*MutationResult, error)
	Scan(scanType ScanType, opts ScanOptions) (*ScanResult, error)
}
