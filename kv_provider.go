package gocb

type kvProvider interface {
	Add(*kvOpManager) (*MutationResult, error)     // Done
	Set(*kvOpManager) (*MutationResult, error)     // Done
	Replace(*kvOpManager) (*MutationResult, error) // Done
	Get(*kvOpManager) (*GetResult, error)          // Done

	GetReplica(*kvOpManager) (*GetReplicaResult, error) // Done
	//GetOneReplica(opts GetOptions) (*GetResult, error)
	Exists(*kvOpManager) (*ExistsResult, error)   // Done
	Delete(*kvOpManager) (*MutationResult, error) //Done

	LookupIn(opts LookupInResult) (*LookupInResult, error)
	MutateIn(opts MutateInOptions) (*MutateInResult, error)

	GetAndTouch(*kvOpManager) (*GetResult, error) // Done
	GetAndLock(*kvOpManager) (*GetResult, error)  // Done
	Unlock(*kvOpManager) error                    // Done
	Touch(*kvOpManager) (*MutationResult, error)  // Done

	Increment(opts IncrementOptions) (*CounterResult, error)
	Decrement(opts DecrementOptions) (*CounterResult, error)

	// Subdoc actions
	Append(*kvOpManager) (*MutationResult, error)  // Done
	Prepend(*kvOpManager) (*MutationResult, error) // Done
	Scan(scanType ScanType, opts ScanOptions) (*ScanResult, error)
}
