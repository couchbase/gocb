package gocbcore

type Cas uint64

type VbUuid uint64
type SeqNo uint64

type MutationToken struct {
	VbUuid VbUuid
	SeqNo  SeqNo
}

type SingleServerStats struct {
	Stats map[string]string
	Error error
}

type PendingOp interface {
	Cancel() bool
}

type multiPendingOp struct {
	ops []PendingOp
}

func (mp *multiPendingOp) Cancel() bool {
	allCancelled := true
	for _, op := range mp.ops {
		if !op.Cancel() {
			allCancelled = false
		}
	}
	return allCancelled
}

func (c *Agent) dispatchOp(req *memdQRequest) (PendingOp, error) {
	err := c.dispatchDirect(req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

type GetCallback func([]byte, uint32, Cas, error)
type UnlockCallback func(Cas, MutationToken, error)
type TouchCallback func(Cas, MutationToken, error)
type RemoveCallback func(Cas, MutationToken, error)
type StoreCallback func(Cas, MutationToken, error)
type CounterCallback func(uint64, Cas, MutationToken, error)
type ObserveCallback func(KeyState, Cas, error)
type ObserveSeqNoCallback func(SeqNo, SeqNo, error)
type GetRandomCallback func([]byte, []byte, uint32, Cas, error)
type ServerStatsCallback func(stats map[string]SingleServerStats)
