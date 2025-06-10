package gocb

// TransactionGetOptions are the options available to the Get operation.
type TransactionGetOptions struct {
	Transcoder Transcoder
}

// TransactionGetReplicaFromPreferredServerGroupOptions are the options available to the GetReplicaFromPreferredServerGroup
// operation.
type TransactionGetReplicaFromPreferredServerGroupOptions struct {
	Transcoder Transcoder
}

// TransactionInsertOptions are the options available to the Insert operation.
type TransactionInsertOptions struct {
	Transcoder Transcoder
}

// TransactionReplaceOptions are the options available to the Replace operation.
type TransactionReplaceOptions struct {
	Transcoder Transcoder
}

// TransactionRemoveOptions are the options available to the Remove operation.
type TransactionRemoveOptions struct {
	Transcoder Transcoder
}
