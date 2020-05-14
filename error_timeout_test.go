package gocb

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
)

func (suite *UnitTestSuite) TestTimeoutError() {
	err := &gocbcore.TimeoutError{
		InnerError:         gocbcore.ErrAmbiguousTimeout,
		OperationID:        "Get",
		Opaque:             "0x09",
		TimeObserved:       100 * time.Millisecond,
		RetryReasons:       []gocbcore.RetryReason{gocbcore.KVLockedRetryReason, gocbcore.CircuitBreakerOpenRetryReason},
		RetryAttempts:      5,
		LastDispatchedTo:   "127.0.0.1:56830",
		LastDispatchedFrom: "127.0.0.1:56839",
		LastConnectionID:   "d323bee8e92a20d6/63ac5d0cc19a1334",
	}

	enhancedErr := maybeEnhanceKVErr(err, "", "", "", "bob")

	if !errors.Is(enhancedErr, ErrTimeout) {
		suite.T().Fatalf("Error should have been ErrTimeout but was %v", enhancedErr)
	}

	var tErr *TimeoutError
	if !errors.As(enhancedErr, &tErr) {
		suite.T().Fatalf("Error should have been TimeoutError but was %v", enhancedErr)
	}

	suite.Assert().Equal(tErr.InnerError, err.InnerError)
	suite.Assert().Equal(tErr.OperationID, err.OperationID)
	suite.Assert().Equal(tErr.Opaque, err.Opaque)
	suite.Assert().Equal(tErr.TimeObserved, err.TimeObserved)
	suite.Assert().Equal(tErr.RetryReasons, []RetryReason{KVLockedRetryReason, CircuitBreakerOpenRetryReason})
	suite.Assert().Equal(tErr.RetryAttempts, err.RetryAttempts)
	suite.Assert().Equal(tErr.LastDispatchedTo, err.LastDispatchedTo)
	suite.Assert().Equal(tErr.LastDispatchedFrom, err.LastDispatchedFrom)
	suite.Assert().Equal(tErr.LastConnectionID, err.LastConnectionID)

	b, mErr := json.Marshal(tErr)
	suite.Require().Nil(mErr, mErr)

	expectedJSON := `{"s":"Get","i":"0x09","t":100000,"rr":["KV_LOCKED","CIRCUIT_BREAKER_OPEN"],"ra":5,"r":"127.0.0.1:56830","l":"127.0.0.1:56839","c":"d323bee8e92a20d6/63ac5d0cc19a1334"}`
	suite.Assert().Equal(expectedJSON, string(b))

	var tErr2 *TimeoutError
	suite.Require().Nil(json.Unmarshal(b, &tErr2))

	// Note that we cannot unmarshal retry reasons or inner error
	suite.Assert().Equal(tErr2.OperationID, err.OperationID)
	suite.Assert().Equal(tErr2.Opaque, err.Opaque)
	suite.Assert().Equal(tErr2.TimeObserved, err.TimeObserved)
	suite.Assert().Equal(tErr2.RetryAttempts, err.RetryAttempts)
	suite.Assert().Equal(tErr2.LastDispatchedTo, err.LastDispatchedTo)
	suite.Assert().Equal(tErr2.LastDispatchedFrom, err.LastDispatchedFrom)
	suite.Assert().Equal(tErr2.LastConnectionID, err.LastConnectionID)
}

func (suite *IntegrationTestSuite) TestTimeoutError_Retries() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("unlockTimeout", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("unlockTimeout", 10, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	err = globalCollection.Unlock("unlockTimeout", 1234, &UnlockOptions{
		Timeout: 100 * time.Millisecond,
	})
	if !errors.Is(err, ErrTimeout) {
		suite.T().Fatalf("Unlock should have errored with ErrTimeout but was %v", err)
	}

	var tErr *TimeoutError
	if errors.As(err, &tErr) {
		suite.Assert().Equal(tErr.OperationID, "Unlock")
		suite.Assert().NotEmpty(tErr.Opaque)
		// Testify doesn't like using Greater with time.Duration
		suite.Assert().Greater(tErr.TimeObserved.Microseconds(), 100*time.Millisecond.Microseconds())
		suite.Assert().Equal(tErr.RetryReasons, []RetryReason{KVTemporaryFailureRetryReason})
		suite.Assert().Greater(tErr.RetryAttempts, uint32(0))
		suite.Assert().NotEmpty(tErr.LastDispatchedTo)
		suite.Assert().NotEmpty(tErr.LastDispatchedFrom)
		suite.Assert().NotEmpty(tErr.LastConnectionID)
	} else {
		suite.T().Fatalf("Error couldn't be asserted to TimeoutError: %v", err)
	}
}
