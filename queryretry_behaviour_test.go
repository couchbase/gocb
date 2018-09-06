package gocb

import (
	"testing"
	"time"
)

func TestQueryDelayRetryBehaviorCanRetry(t *testing.T) {
	behav := NewQueryDelayRetryBehavior(10, 1000, 1*time.Second, QueryLinearDelayFunction)

	var retries uint
	if !behav.CanRetry(retries) {
		t.Log("TestQueryDelayRetryBehaviorLinear should have been able to retry but couldn't")
		t.Fail()
	}

	retries = 9
	if !behav.CanRetry(retries) {
		t.Log("TestQueryDelayRetryBehaviorLinear should have been able to retry but couldn't")
		t.Fail()
	}

	retries = 10
	if behav.CanRetry(retries) {
		t.Log("TestQueryDelayRetryBehaviorLinear shouldn't have been able to retry but could")
		t.Fail()
	}
}

func TestQueryDelayRetryBehaviorLinear(t *testing.T) {
	behav := NewQueryDelayRetryBehavior(10, 2, 500*time.Millisecond, QueryLinearDelayFunction)

	testNextInterval(t, behav, 1, 2*time.Millisecond, "TestQueryDelayRetryBehaviorLinear")
	testNextInterval(t, behav, 5, 10*time.Millisecond, "TestQueryDelayRetryBehaviorLinear")
	testNextInterval(t, behav, 10, 20*time.Millisecond, "TestQueryDelayRetryBehaviorLinear")
	testNextInterval(t, behav, 1000, 500*time.Millisecond, "TestQueryDelayRetryBehaviorLinear")
}

func TestQueryDelayRetryBehaviorExponential(t *testing.T) {
	behav := NewQueryDelayRetryBehavior(10, 2, 500*time.Millisecond, QueryExponentialDelayFunction)

	testNextInterval(t, behav, 1, 2*time.Millisecond, "TestQueryDelayRetryBehaviorExponential")
	testNextInterval(t, behav, 5, 32*time.Millisecond, "TestQueryDelayRetryBehaviorExponential")
	testNextInterval(t, behav, 10, 500*time.Millisecond, "TestQueryDelayRetryBehaviorExponential")
}

func testNextInterval(t *testing.T, behav *QueryDelayRetryBehavior, retries uint, expected time.Duration, testName string) {
	interval := behav.NextInterval(retries)
	if interval != expected {
		t.Logf("%s expected interval of %v but was %v", testName, expected, interval)
		t.Fail()
	}
}
