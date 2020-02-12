package gocb

func (suite *IntegrationTestSuite) TestBinaryAppend() {
	if !globalCluster.SupportsFeature(AdjoinFeature) {
		suite.T().Skip("Skipping due to serverside bug")
	}
	colBinary := globalCollection.Binary()

	tcoder := NewRawBinaryTranscoder()
	res, err := globalCollection.Upsert("binaryAppend", []byte("foo"), &UpsertOptions{
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Upsert, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	appendRes, err := colBinary.Append("binaryAppend", []byte("bar"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to Append, err: %v", err)
	}

	if appendRes.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	appendDoc, err := globalCollection.Get("binaryAppend", &GetOptions{
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var appendContent []byte
	err = appendDoc.Content(&appendContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if string(appendContent) != "foobar" {
		suite.T().Fatalf("Expected append result to be foobar but was %s", appendContent)
	}
}

func (suite *IntegrationTestSuite) TestBinaryPrepend() {
	if !globalCluster.SupportsFeature(AdjoinFeature) {
		suite.T().Skip("Skipping due to serverside bug")
	}
	colBinary := globalCollection.Binary()

	tcoder := NewRawBinaryTranscoder()
	res, err := globalCollection.Upsert("binaryPrepend", []byte("foo"), &UpsertOptions{
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Upsert, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	appendRes, err := colBinary.Prepend("binaryPrepend", []byte("bar"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to Append, err: %v", err)
	}

	if appendRes.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	appendDoc, err := globalCollection.Get("binaryPrepend", &GetOptions{
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var appendContent []byte
	err = appendDoc.Content(&appendContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if string(appendContent) != "barfoo" {
		suite.T().Fatalf("Expected prepend result to be boofar but was %s", appendContent)
	}
}

func (suite *IntegrationTestSuite) TestBinaryIncrement() {
	colBinary := globalCollection.Binary()

	res, err := colBinary.Increment("binaryIncrement", &IncrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 0 {
		suite.T().Fatalf("Expected counter value to be 0 but was %d", res.Content())
	}

	res, err = colBinary.Increment("binaryIncrement", &IncrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 10 {
		suite.T().Fatalf("Expected counter value to be 10 but was %d", res.Content())
	}

	res, err = colBinary.Increment("binaryIncrement", &IncrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 20 {
		suite.T().Fatalf("Expected counter value to be 20 but was %d", res.Content())
	}

	incrementDoc, err := globalCollection.Get("binaryIncrement", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var incrementContent int
	err = incrementDoc.Content(&incrementContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if incrementContent != 20 {
		suite.T().Fatalf("Expected counter value to be 20 but was %d", res.Content())
	}
}

func (suite *IntegrationTestSuite) TestBinaryDecrement() {
	colBinary := globalCollection.Binary()

	res, err := colBinary.Decrement("binaryDecrement", &DecrementOptions{
		Delta:   10,
		Initial: 100,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 100 {
		suite.T().Fatalf("Expected counter value to be 100 but was %d", res.Content())
	}

	res, err = colBinary.Decrement("binaryDecrement", &DecrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 90 {
		suite.T().Fatalf("Expected counter value to be 90 but was %d", res.Content())
	}

	res, err = colBinary.Decrement("binaryDecrement", &DecrementOptions{
		Delta: 10,
	})
	if err != nil {
		suite.T().Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		suite.T().Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 80 {
		suite.T().Fatalf("Expected counter value to be 80 but was %d", res.Content())
	}

	incrementDoc, err := globalCollection.Get("binaryDecrement", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var incrementContent int
	err = incrementDoc.Content(&incrementContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if incrementContent != 80 {
		suite.T().Fatalf("Expected counter value to be 80 but was %d", res.Content())
	}
}
