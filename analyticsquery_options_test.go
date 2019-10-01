package gocb

import (
	"testing"
	"time"
)

func TestAnalyticsQueryOptionsEmpty(t *testing.T) {
	opts := &AnalyticsOptions{}

	statement := "select * from default"
	optMap, err := opts.toMap(statement)
	if err != nil {
		t.Fatalf("Expected no error but was %v", err)
	}

	if len(optMap) != 2 {
		t.Fatalf("Expected optMap to be len 1 but was %v", optMap)
	}

	testAssertOption(t, statement, "statement", optMap)

	// Expecting a UUID here so just check it isn't empty
	val, ok := optMap["client_context_id"]
	if !ok {
		t.Fatalf("Expected client_context_id to be non nil")
	}

	if val == "" {
		t.Fatalf("Expected client_context_id to be non empty")
	}
}

func TestAnalyticsQueryOptionsTimeout(t *testing.T) {
	timeout := 60 * time.Second
	opts := &AnalyticsOptions{
		ServerSideTimeout: timeout,
	}

	statement := "select * from default"
	optMap, err := opts.toMap(statement)
	if err != nil {
		t.Fatalf("Expected no error but was %v", err)
	}

	testAssertOption(t, statement, "statement", optMap)
	testAssertOption(t, timeout.String(), "timeout", optMap)
}

func TestAnalyticsQueryOptionsClientContextID(t *testing.T) {
	id := "62d29101-0c9f-400d-af2b-9bd44a557a7c"
	opts := &AnalyticsOptions{
		ClientContextID: id,
	}

	statement := "select * from default"
	optMap, err := opts.toMap(statement)
	if err != nil {
		t.Fatalf("Expected no error but was %v", err)
	}

	testAssertOption(t, statement, "statement", optMap)
	testAssertOption(t, id, "client_context_id", optMap)
}

func TestAnalyticsQueryOptionsPriority(t *testing.T) {
	opts := &AnalyticsOptions{
		Priority: true,
	}

	statement := "select * from default"
	optMap, err := opts.toMap(statement)
	if err != nil {
		t.Fatalf("Expected no error but was %v", err)
	}

	testAssertOption(t, statement, "statement", optMap)
	testAssertOption(t, -1, "priority", optMap)
}

func TestAnalyticsQueryOptionsPositionalParams(t *testing.T) {
	params := []interface{}{1, "imafish"}
	opts := &AnalyticsOptions{
		PositionalParameters: params,
	}

	statement := "select * from default"
	optMap, err := opts.toMap(statement)
	if err != nil {
		t.Fatalf("Expected no error but was %v", err)
	}

	testAssertOption(t, statement, "statement", optMap)

	args, ok := optMap["args"]
	if !ok {
		t.Fatalf("Options had no args")
	}
	castArgs, ok := args.([]interface{})
	if !ok {
		t.Fatalf("Options was not []interface{}")
	}
	if len(castArgs) != len(params) {
		t.Fatalf("Expected positional params len to be %d but was %d", len(params), len(castArgs))
	}
	for i, v := range opts.PositionalParameters {
		if castArgs[i] != v {
			t.Fatalf("Positional parameters index %d should have been %v, was %v", i, v, castArgs[i])
		}
	}
}

func TestAnalyticsQueryOptionsNamedParams(t *testing.T) {
	params := map[string]interface{}{
		"num":     1,
		"imafish": "namedbarry",
		"$cilit":  "bang",
	}
	opts := &AnalyticsOptions{
		NamedParameters: params,
	}

	statement := "select * from default"
	optMap, err := opts.toMap(statement)
	if err != nil {
		t.Fatalf("Expected no error but was %v", err)
	}

	testAssertOption(t, statement, "statement", optMap)

	numVal, ok := optMap["$num"]
	if !ok {
		t.Fatalf("Expected args to contain $num, was %v", optMap)
	}

	if numVal != 1 {
		t.Fatalf("Expected numVal to be 1 but was %d", numVal)
	}

	fishVal, ok := optMap["$imafish"]
	if !ok {
		t.Fatalf("Expected args to contain $imafish, was %v", optMap)
	}

	if fishVal != "namedbarry" {
		t.Fatalf("Expected fishVal to be namedbarry but was %s", fishVal)
	}

	cilitVal, ok := optMap["$cilit"]
	if !ok {
		t.Fatalf("Expected args to contain $cilit, was %v", optMap)
	}

	if cilitVal != "bang" {
		t.Fatalf("Expected cilitVal to be bang but was %s", cilitVal)
	}
}

func TestAnalyticsQueryOptionsAllTheParams(t *testing.T) {
	opts := &AnalyticsOptions{
		NamedParameters: map[string]interface{}{
			"num":     1,
			"imafish": "namedbarry",
			"$cilit":  "bang",
		},
		PositionalParameters: []interface{}{1, "imafish"},
	}

	statement := "select * from default"
	_, err := opts.toMap(statement)
	if err == nil {
		t.Fatalf("Expected an error but was none")
	}
}

func TestAnalyticsQueryOptionsRawParam(t *testing.T) {
	params := map[string]interface{}{
		"raw": "param",
	}
	opts := &AnalyticsOptions{
		Raw: params,
	}

	statement := "select * from default"
	optMap, err := opts.toMap(statement)
	if err != nil {
		t.Fatalf("Expected no error but was %v", err)
	}

	testAssertOption(t, statement, "statement", optMap)

	rawVal, ok := optMap["raw"]
	if !ok {
		t.Fatalf("Expected args to contain rawVal, was %v", optMap)
	}

	if rawVal != "param" {
		t.Fatalf("Expected numVal to be param but was %d", rawVal)
	}
}

func TestAnalyticsQueryOptionsReadOnly(t *testing.T) {
	opts := &AnalyticsOptions{
		ReadOnly: true,
	}

	statement := "select * from default"
	optMap, err := opts.toMap(statement)
	if err != nil {
		t.Fatalf("Expected no error but was %v", err)
	}

	testAssertOption(t, statement, "statement", optMap)
	testAssertOption(t, true, "readonly", optMap)
}
