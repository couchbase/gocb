package gocb

type kvProvider interface {
	Add(*kvOpManager) (*MutationResult, error)     // Done
	Set(*kvOpManager) (*MutationResult, error)     // Done
	Replace(*kvOpManager) (*MutationResult, error) // Done
	Get(*kvOpManager) (*GetResult, error)          // Done

	GetReplica(*kvOpManager) (*GetReplicaResult, error)
	//GetOneReplica(opts GetOptions) (*GetResult, error)
	Exists(*kvOpManager) (*ExistsResult, error)   // Done
	Delete(*kvOpManager) (*MutationResult, error) //Done

	LookupIn(opts LookupInResult) (*LookupInResult, error)
	MutateIn(opts MutateInOptions) (*MutateInResult, error)

	GetAndTouch(*kvOpManager) (*GetResult, error) // Done
	GetAndLock(*kvOpManager) (*GetResult, error)  // Done
	Unlock(*kvOpManager) error                    // Done
	Touch(*kvOpManager) (*MutationResult, error)  // Done

	// Subdoc actions
	Increment(opts IncrementOptions) (*CounterResult, error)
	Decrement(opts DecrementOptions) (*CounterResult, error)

	Append(opts AppendOptions) (*MutationResult, error)
	Prepend(opts PrependOptions) (*MutationResult, error)
	Scan(scanType ScanType, opts ScanOptions) (*ScanResult, error)
}
