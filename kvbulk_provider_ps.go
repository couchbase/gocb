package gocb

import (
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
)

type kvBulkProviderPs struct {
	client kv_v1.KvServiceClient
}

func (p *kvBulkProviderPs) Do(c *Collection, ops []BulkOp, opts *BulkOpOptions) error {
	return ErrFeatureNotAvailable
}
