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
	Scan(ScanType, *kvOpManager) (*ScanResult, error)
}
