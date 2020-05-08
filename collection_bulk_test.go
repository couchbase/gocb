package gocb

import (
	"fmt"
)

func (suite *IntegrationTestSuite) TestUpsertGetBulk() {
	suite.skipIfUnsupported(KeyValueFeature)

	var ops []BulkOp
	for i := 0; i < 20; i++ {
		ops = append(ops, &UpsertOp{
			ID:     fmt.Sprintf("%d", i),
			Value:  "test",
			Expiry: 20,
		})
	}

	err := globalCollection.Do(ops, nil)
	if err != nil {
		suite.T().Fatalf("Expected Do to not error for upserts %v", err)
	}

	for _, op := range ops {
		upsertOp, ok := op.(*UpsertOp)
		if !ok {
			suite.T().Fatalf("Could not type assert BulkOp into UpsertOp")
		}

		if upsertOp.Err != nil {
			suite.T().Fatalf("Expected UpsertOp Err to be nil but was %v", upsertOp.Err)
		}

		if upsertOp.Result.Cas() == 0 {
			suite.T().Fatalf("Expected UpsertOp Cas to be non zero")
		}
	}

	var getOps []BulkOp
	for i := 0; i < 20; i++ {
		getOps = append(getOps, &GetOp{
			ID: fmt.Sprintf("%d", i),
		})
	}

	err = globalCollection.Do(getOps, nil)
	if err != nil {
		suite.T().Fatalf("Expected Do to not error for gets %v", err)
	}

	for _, op := range getOps {
		getOp, ok := op.(*GetOp)
		if !ok {
			suite.T().Fatalf("Could not type assert BulkOp into GetOp")
		}

		if getOp.Err != nil {
			suite.T().Fatalf("Expected GetOp Err to be nil but was %v", getOp.Err)
		}

		if getOp.Result.Cas() == 0 {
			suite.T().Fatalf("Expected GetOp Cas to be non zero")
		}

		var val string
		err = getOp.Result.Content(&val)
		if err != nil {
			suite.T().Fatalf("Failed to get content from GetOp %v", err)
		}

		if val != "test" {
			suite.T().Fatalf("Expected GetOp value to be test but was %s", val)
		}
	}
}
func (suite *IntegrationTestSuite) TestInsertDocsBulk() {
	suite.skipIfUnsupported(KeyValueFeature)

	var ops []BulkOp
	for i := 0; i < 20; i++ {
		ops = append(ops, &InsertOp{
			ID:     fmt.Sprintf("insert-docs-bulk-%d", i),
			Value:  "test",
			Expiry: 20,
		})
	}

	err := globalCollection.Do(ops, nil)
	if err != nil {
		suite.T().Fatalf("Expected Do to not error for inserts %v", err)
	}

	for _, op := range ops {
		insertOp, ok := op.(*InsertOp)
		if !ok {
			suite.T().Fatalf("Could not type assert BulkOp into InsertOp")
		}

		if insertOp.Err != nil {
			suite.T().Fatalf("Expected UpsertOp Err to be nil but was %v", insertOp.Err)
		}

	}
}

func (suite *IntegrationTestSuite) TestReplaceOperationBulk() {
	suite.skipIfUnsupported(KeyValueFeature)

	var ops []BulkOp
	for i := 0; i < 20; i++ {
		ops = append(ops, &UpsertOp{
			ID:     fmt.Sprintf("replace-docs-bulk-%d", i),
			Value:  "test",
			Expiry: 20,
		})
	}

	err := globalCollection.Do(ops, nil)
	if err != nil {
		suite.T().Fatalf("Expected Do to not error for upserts %v", err)
	}

	for _, op := range ops {
		upsertOp, ok := op.(*UpsertOp)
		if !ok {
			suite.T().Fatalf("Could not type assert BulkOp into UpsertOp")
		}

		if upsertOp.Err != nil {
			suite.T().Fatalf("Expected UpsertOp Err to be nil but was %v", upsertOp.Err)
		}

	}

	var replaceOps []BulkOp
	for i := 0; i < 20; i++ {
		replaceOps = append(replaceOps, &ReplaceOp{
			ID: fmt.Sprintf("replace-docs-bulk-%d", i),
		})
	}

	err = globalCollection.Do(replaceOps, nil)
	if err != nil {
		suite.T().Fatalf("Expected Do to not error for replace %v", err)
	}

	for _, op := range replaceOps {
		replaceOp, ok := op.(*ReplaceOp)
		if !ok {
			suite.T().Fatalf("Could not type assert BulkOp into ReplaceOp")
		}

		if replaceOp.Err != nil {
			suite.T().Fatalf("Expected UpsertOp Err to be nil but was %v", replaceOp.Err)
		}

	}
}

func (suite *IntegrationTestSuite) TestRemoveOperationBulk() {
	suite.skipIfUnsupported(KeyValueFeature)

	var ops []BulkOp
	for i := 0; i < 20; i++ {
		ops = append(ops, &UpsertOp{
			ID:     fmt.Sprintf("remove-docs-bulk-%d", i),
			Value:  "test",
			Expiry: 20,
		})
	}

	err := globalCollection.Do(ops, nil)
	if err != nil {
		suite.T().Fatalf("Expected Do to not error for upserts %v", err)
	}

	for _, op := range ops {
		upsertOp, ok := op.(*UpsertOp)
		if !ok {
			suite.T().Fatalf("Could not type assert BulkOp into UpsertOp")
		}

		if upsertOp.Err != nil {
			suite.T().Fatalf("Expected UpsertOp Err to be nil but was %v", upsertOp.Err)
		}

		if upsertOp.Result.Cas() == 0 {
			suite.T().Fatalf("Expected UpsertOp Cas to be non zero")
		}
	}

	var removeOps []BulkOp
	for i := 0; i < 20; i++ {
		removeOps = append(removeOps, &RemoveOp{
			ID: fmt.Sprintf("remove-docs-bulk-%d", i),
		})
	}

	err = globalCollection.Do(removeOps, nil)
	if err != nil {
		suite.T().Fatalf("Expected Do to not error for removeops %v", err)
	}

	for _, op := range removeOps {
		removeOp, ok := op.(*RemoveOp)
		if !ok {
			suite.T().Fatalf("Could not type assert BulkOp into RemoveOp")
		}

		if removeOp.Err != nil {
			suite.T().Fatalf("Expected RemoveOp Err to be nil but was %v", removeOp.Err)
		}

		if removeOp.Result.Cas() == 0 {
			suite.T().Fatalf("Expected RemoveOp Cas to be non zero")
		}
	}
}
