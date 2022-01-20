package gocb

// TransactionSerializedContext represents a transaction which has been serialized
// for resumption at a later point in time.
type TransactionSerializedContext struct {
}

// EncodeAsString will encode this TransactionSerializedContext to a string which
// can be decoded later to resume the transaction.
func (c *TransactionSerializedContext) EncodeAsString() string {
	return ""
}

// EncodeAsBytes will encode this TransactionSerializedContext to a set of bytes which
// can be decoded later to resume the transaction.
func (c *TransactionSerializedContext) EncodeAsBytes() []byte {
	return []byte{}
}
