package gocb

import (
	"context"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// ScanOptions are the set of options available to the Scan operation.
// VOLATILE: This API is subject to change at any time.
type ScanOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	IDsOnly        bool
	ConsistentWith *MutationState
	Sort           ScanSort

	// BatchByteLimit specifies a limit to how many bytes are sent from server to client on each partition batch.
	BatchByteLimit uint32
	// BatchItemLimit specifies a limit to how many items are sent from server to client on each partition batch.
	BatchItemLimit uint32

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// ScanSort represents the sort order of a Scan operation.
type ScanSort uint8

const (
	// ScanSortNone indicates that no sorting should be applied during a Scan operation.
	ScanSortNone ScanSort = iota

	// ScanSortAscending indicates that ascending sort should be applied during a Scan operation.
	ScanSortAscending
)

// ScanTerm represents a term that can be used during a Scan operation.
type ScanTerm struct {
	Term      string
	Exclusive bool
}

// ScanTermMinimum represents the minimum value that a ScanTerm can represent.
func ScanTermMinimum() *ScanTerm {
	return &ScanTerm{
		Term: "\x00",
	}
}

// ScanTermMaximum represents the maximum value that a ScanTerm can represent.
func ScanTermMaximum() *ScanTerm {
	return &ScanTerm{
		Term: "\xFF",
	}
}

// ScanType represents the mode of execution to use for a Scan operation.
type ScanType interface {
	isScanType()
}

// NewRangeScanForPrefix creates a new range scan for the given prefix, starting at the prefix and ending at the prefix
// plus maximum.
// VOLATILE: This API is subject to change at any time.
func NewRangeScanForPrefix(prefix string) RangeScan {
	return RangeScan{
		From: &ScanTerm{
			Term: prefix,
		},
		To: &ScanTerm{
			Term: prefix + "\xFF",
		},
	}
}

// RangeScan indicates that the Scan operation should scan a range of keys.
type RangeScan struct {
	From *ScanTerm
	To   *ScanTerm
}

func (rs RangeScan) isScanType() {}

func (rs RangeScan) toCore() (*gocbcore.RangeScanCreateRangeScanConfig, error) {
	to := rs.To
	from := rs.From

	rangeOptions := &gocbcore.RangeScanCreateRangeScanConfig{}
	if from.Exclusive {
		rangeOptions.ExclusiveStart = []byte(from.Term)
	} else {
		rangeOptions.Start = []byte(from.Term)
	}
	if to.Exclusive {
		rangeOptions.ExclusiveEnd = []byte(to.Term)
	} else {
		rangeOptions.End = []byte(to.Term)
	}

	return rangeOptions, nil
}

// SamplingScan indicates that the Scan operation should perform random sampling.
type SamplingScan struct {
	Limit uint64
	Seed  uint64
}

func (rs SamplingScan) isScanType() {}

func (rs SamplingScan) toCore() (*gocbcore.RangeScanCreateRandomSamplingConfig, error) {
	if rs.Limit == 0 {
		return nil, makeInvalidArgumentsError("sampling scan limit must be greater than 0")
	}

	return &gocbcore.RangeScanCreateRandomSamplingConfig{
		Samples: rs.Limit,
		Seed:    rs.Seed,
	}, nil
}

// Scan performs a scan across a Collection, returning a stream of documents.
// VOLATILE: This API is subject to change at any time.
func (c *Collection) Scan(scanType ScanType, opts *ScanOptions) (*ScanResult, error) {
	if opts == nil {
		opts = &ScanOptions{}
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = c.timeoutsConfig.KVScanTimeout
	}

	config, err := c.waitForConfigSnapshot(opts.Context, time.Now().Add(timeout), agent)
	if err != nil {
		return nil, err
	}

	numVbuckets, err := config.NumVbuckets()
	if err != nil {
		return nil, err
	}

	if numVbuckets == 0 {
		return nil, makeInvalidArgumentsError("can only use RangeScan with couchbase buckets")
	}

	opm, err := c.newRangeScanOpManager(scanType, numVbuckets, agent, opts.ParentSpan, opts.ConsistentWith,
		opts.IDsOnly, opts.Sort)
	if err != nil {
		return nil, err
	}

	opm.SetTranscoder(opts.Transcoder)
	opm.SetContext(opts.Context)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetTimeout(opts.Timeout)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetItemLimit(opts.BatchItemLimit)
	opm.SetByteLimit(opts.BatchByteLimit)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	return opm.Scan()
}

func (c *Collection) waitForConfigSnapshot(ctx context.Context, deadline time.Time, agent kvProvider) (snapOut *gocbcore.ConfigSnapshot, errOut error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opm := newAsyncOpManager(ctx)
	err := opm.Wait(agent.WaitForConfigSnapshot(deadline, gocbcore.WaitForConfigSnapshotOptions{}, func(result *gocbcore.WaitForConfigSnapshotResult, err error) {
		if err != nil {
			errOut = err
			opm.Reject()
			return
		}

		snapOut = result.Snapshot
		opm.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}
