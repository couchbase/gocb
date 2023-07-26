package gocb

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/mock"
)

type mockConfigSnapshotProvider struct {
	snapshot *mockConfigSnapshot
}

func (p *mockConfigSnapshotProvider) WaitForConfigSnapshot(ctx context.Context, deadline time.Time) (coreConfigSnapshot, error) {
	return p.snapshot, nil
}

type mockConfigSnapshot struct {
	revID       int64
	numVbuckets int
	numReplicas int
}

func (p *mockConfigSnapshot) RevID() int64 {
	return p.revID
}

func (p *mockConfigSnapshot) NumVbuckets() (int, error) {
	return p.numVbuckets, nil
}

func (p *mockConfigSnapshot) NumReplicas() (int, error) {
	return p.numReplicas, nil
}

func (suite *UnitTestSuite) TestScanAllScansTmpFailAtCreate() {
	test := func(scan ScanType) (*ScanResult, error) {
		start := time.Now()

		opts := &ScanOptions{
			IDsOnly: true,
			Timeout: 1 * time.Millisecond,
		}

		provider := makeRangeScanProvider(func(args mock.Arguments) {
			cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

			if time.Now().Sub(start) > opts.Timeout {
				cb(nil, ErrTimeout)
				return
			}
			cb(nil, ErrTemporaryFailure)
		})

		snap := &mockConfigSnapshot{numVbuckets: 8}

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		return agent.Scan(col, scan, opts)
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		res, err := test(scan)
		suite.Require().NoError(err)

		ids := suite.iterateRangeScan(res)

		suite.Assert().Empty(ids)
		suite.Require().NoError(res.Err())
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		_, err := test(scan)
		suite.Require().ErrorIs(err, ErrTimeout)
	})
}

func (suite *UnitTestSuite) TestScanAllScansEmpty() {
	test := func(scan ScanType) {
		provider := new(mockKvProviderCoreProvider)
		provider.
			On(
				"RangeScanCreate",
				mock.AnythingOfType("uint16"),
				mock.AnythingOfType("gocbcore.RangeScanCreateOptions"),
				mock.AnythingOfType("gocbcore.RangeScanCreateCallback"),
			).
			Run(func(args mock.Arguments) {
				cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

				cb(nil, gocbcore.ErrDocumentNotFound)
			}).
			Return(new(mockPendingOp), nil)

		opts := &ScanOptions{
			IDsOnly: true,
		}

		snap := &mockConfigSnapshot{numVbuckets: 8}

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		res, err := agent.Scan(col, scan, opts)
		suite.Require().Nil(err, err)

		ids := make(map[string]struct{})
		for {
			d := res.Next()
			if d == nil {
				break
			}

			ids[d.ID()] = struct{}{}
		}

		suite.Assert().Empty(ids)
		suite.Require().Nil(res.Err())
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		test(scan)
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		test(scan)
	})
}

type rangeScanCreateResult struct {
	keysOnly bool
	items    []gocbcore.RangeScanItem

	continueRunFunc func(gocbcore.RangeScanContinueDataCallback, gocbcore.RangeScanContinueActionCallback)

	wasCancelled bool
}

func (r *rangeScanCreateResult) ScanUUID() []byte {
	return []byte("scanuuid")
}

func (r *rangeScanCreateResult) KeysOnly() bool {
	return r.keysOnly
}

func (r *rangeScanCreateResult) RangeScanContinue(opts gocbcore.RangeScanContinueOptions, dataCb gocbcore.RangeScanContinueDataCallback, actionCb gocbcore.RangeScanContinueActionCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(1*time.Microsecond, func() {
		if r.continueRunFunc == nil {
			dataCb(r.items)
			actionCb(&gocbcore.RangeScanContinueResult{
				Complete: true,
			}, nil)
		} else {
			r.continueRunFunc(dataCb, actionCb)
		}
	})

	return &mockPendingOp{}, nil
}

func (r *rangeScanCreateResult) RangeScanCancel(opts gocbcore.RangeScanCancelOptions, cb gocbcore.RangeScanCancelCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(1*time.Microsecond, func() {
		r.wasCancelled = true
		cb(&gocbcore.RangeScanCancelResult{}, nil)
	})

	return &mockPendingOp{}, nil
}

func (suite *UnitTestSuite) TestScanFirstCreateFailsUnknownError() {
	expectedErr := errors.New("unknown error")
	test := func(scan ScanType) (*ScanResult, error) {
		var calls uint32
		provider := makeRangeScanProvider(func(args mock.Arguments) {
			cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

			if atomic.AddUint32(&calls, 1) == 1 {
				cb(nil, expectedErr)
				return
			}

			cb(&rangeScanCreateResult{
				items: []gocbcore.RangeScanItem{
					{
						Key: []byte(uuid.NewString()[:6]),
					},
				},
			}, nil)
		})

		snap := &mockConfigSnapshot{numVbuckets: 4}

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		opts := &ScanOptions{
			IDsOnly: true,
		}

		return agent.Scan(col, scan, opts)
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		res, err := test(scan)
		suite.Require().NoError(err)

		ids := suite.iterateRangeScan(res)

		suite.Assert().Len(ids, 3)
		suite.Require().NoError(res.Err())
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		_, err := test(scan)
		suite.Require().ErrorIs(err, expectedErr)
	})
}

func (suite *UnitTestSuite) TestScanSecondCreateFailsUnknownError() {
	expectedErr := errors.New("unknown error")
	test := func(scan ScanType) (*ScanResult, error) {
		var calls uint32
		provider := makeRangeScanProvider(func(args mock.Arguments) {
			cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

			if atomic.AddUint32(&calls, 1) == 2 {
				cb(nil, expectedErr)
				return
			}

			cb(&rangeScanCreateResult{
				items: []gocbcore.RangeScanItem{
					{
						Key: []byte(uuid.NewString()[:6]),
					},
				},
			}, nil)
			return
		})

		snap := &mockConfigSnapshot{numVbuckets: 4}

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		opts := &ScanOptions{
			IDsOnly: true,
		}

		return agent.Scan(col, scan, opts)
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		res, err := test(scan)
		suite.Require().NoError(err)

		ids := suite.iterateRangeScan(res)

		suite.Assert().Len(ids, 3)
		suite.Require().NoError(res.Err())
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		res, err := test(scan)
		suite.Require().NoError(err)

		ids := suite.iterateRangeScan(res)

		suite.Assert().Len(ids, 1)
		suite.Require().ErrorIs(expectedErr, res.Err())
	})
}

func (suite *UnitTestSuite) TestScanNMV() {
	lastTermSeen := "lasttermseen"
	keyAfter := "keyafter"
	test := func(scan ScanType) (map[string]struct{}, string, error) {
		var firstTermAfter string
		var calls uint32
		var vbID int16 = -1
		var createCalls uint32
		provider := makeRangeScanProvider(func(args mock.Arguments) {
			thisVBID := int16(args.Get(0).(uint16))
			opts := args.Get(1).(gocbcore.RangeScanCreateOptions)
			cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

			if atomic.AddUint32(&calls, 1) == 2 || thisVBID == vbID {
				vbID = thisVBID
				if createCalls == 0 {
					createCalls++
					var contCalls uint32
					cb(&rangeScanCreateResult{
						continueRunFunc: func(dataCb gocbcore.RangeScanContinueDataCallback, actionCb gocbcore.RangeScanContinueActionCallback) {
							contCalls++
							if contCalls == 1 {
								dataCb([]gocbcore.RangeScanItem{
									{
										Key: []byte(uuid.NewString()[:6]),
									},
									{
										Key: []byte(lastTermSeen),
									},
								})
								actionCb(&gocbcore.RangeScanContinueResult{
									More: true,
								}, nil)
							} else {
								actionCb(nil, gocbcore.ErrNotMyVBucket)
							}
						},
					}, nil)
				} else {
					if opts.Range != nil {
						firstTermAfter = string(opts.Range.Start)
					}
					cb(&rangeScanCreateResult{
						continueRunFunc: func(dataCb gocbcore.RangeScanContinueDataCallback, actionCb gocbcore.RangeScanContinueActionCallback) {
							dataCb([]gocbcore.RangeScanItem{
								{
									Key: []byte(keyAfter),
								},
							})
							actionCb(&gocbcore.RangeScanContinueResult{
								Complete: true,
							}, nil)
						},
					}, nil)
				}
				return
			}

			cb(&rangeScanCreateResult{
				items: []gocbcore.RangeScanItem{
					{
						Key: []byte(uuid.NewString()[:6]),
					},
				},
			}, nil)
			return
		})

		snap := &mockConfigSnapshot{numVbuckets: 4}

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		opts := &ScanOptions{
			IDsOnly: true,
		}

		res, err := agent.Scan(col, scan, opts)
		if err != nil {
			return nil, "", err
		}

		ids := suite.iterateRangeScan(res)

		return ids, firstTermAfter, res.Err()
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		ids, _, err := test(scan)
		suite.Require().NoError(err)

		// 3 vbuckets will yield 1 item,  the other will yield 2 items on the first continue, then 1 further on the
		// continue after the NMV.
		suite.Assert().Len(ids, 6)
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		ids, firstTermAfter, err := test(scan)
		suite.Require().NoError(err)

		// 3 vbuckets will yield 1 item,  the other will yield 2 items on the first continue, then 1 further on the
		// continue after the NMV.
		suite.Assert().Len(ids, 6)
		// A NMV error should trigger a new create with the start being the last term seen.
		suite.Assert().Equal(lastTermSeen, firstTermAfter)
	})
}

func (suite *UnitTestSuite) iterateRangeScan(res *ScanResult) map[string]struct{} {
	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		suite.Assert().NotEmpty(d.ID())

		ids[d.ID()] = struct{}{}
	}

	return ids
}

func makeRangeScanProvider(createCb func(args mock.Arguments)) *mockKvProviderCoreProvider {
	provider := new(mockKvProviderCoreProvider)
	provider.
		On(
			"RangeScanCreate",
			mock.AnythingOfType("uint16"),
			mock.AnythingOfType("gocbcore.RangeScanCreateOptions"),
			mock.AnythingOfType("gocbcore.RangeScanCreateCallback"),
		).
		Run(createCb).
		Return(new(mockPendingOp), nil)

	return provider
}
