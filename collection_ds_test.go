package gocb

func (suite *IntegrationTestSuite) TestListCrud() {
	suite.skipIfUnsupported(KeyValueFeature)

	list := globalCollection.List("testList")
	err := list.Append("test1")
	if err != nil {
		suite.T().Fatalf("Failed to append to list %v", err)
	}
	err = list.Append("test2")
	if err != nil {
		suite.T().Fatalf("Failed to append to list %v", err)
	}
	err = list.Prepend("test3")
	if err != nil {
		suite.T().Fatalf("Failed to append to list %v", err)
	}
	err = list.Append("test4")
	if err != nil {
		suite.T().Fatalf("Failed to append to list %v", err)
	}

	size, err := list.Size()
	if err != nil {
		suite.T().Fatalf("Failed to get size of list %v", err)
	}

	if size != 4 {
		suite.T().Fatalf("Expected list size to be 4 but was %d", size)
	}

	index, err := list.IndexOf("test2")
	if err != nil {
		suite.T().Fatalf("Failed to get index from list %v", err)
	}

	if index != 2 {
		suite.T().Fatalf("Expected list index to be 2 but was %d", size)
	}

	var index2 string
	err = list.At(2, &index2)
	if err != nil {
		suite.T().Fatalf("Failed to get content from list %v", err)
	}

	if index2 != "test2" {
		suite.T().Fatalf("Expected list item to be test2 but was %d", size)
	}

	iter, err := list.Iterator()
	if err != nil {
		suite.T().Fatalf("Failed to get iterator for list %v", err)
	}

	var items []string
	for _, item := range iter {
		items = append(items, item.(string))
	}

	expected := []string{"test3", "test1", "test2", "test4"}
	for i, item := range items {
		if expected[i] != item {
			suite.T().Fatalf("Expected item at %d to be %s but was %s", i, expected[i], item)
		}
	}

	err = list.RemoveAt(2)
	if err != nil {
		suite.T().Fatalf("Failed to remove from list %v", err)
	}

	size, err = list.Size()
	if err != nil {
		suite.T().Fatalf("Failed to get size of list %v", err)
	}

	if size != 3 {
		suite.T().Fatalf("Expected size after remove to be 3 but was %d", size)
	}

	iter2, err := list.Iterator()
	if err != nil {
		suite.T().Fatalf("Failed to get iterator for list %v", err)
	}

	var items2 []string
	for _, item := range iter2 {
		items2 = append(items2, item.(string))
	}

	expected2 := []string{"test3", "test1", "test4"}
	for i, item := range items2 {
		if expected2[i] != item {
			suite.T().Fatalf("Expected item at %d to be %s but was %s", i, expected2[i], item)
		}
	}

	err = list.Clear()
	if err != nil {
		suite.T().Fatalf("Failed to clear list %v", err)
	}
}

func (suite *IntegrationTestSuite) TestSetCrud() {
	suite.skipIfUnsupported(KeyValueFeature)

	set := globalCollection.Set("testSet")
	err := set.Add("test1")
	if err != nil {
		suite.T().Fatalf("Failed to add to set %v", err)
	}
	err = set.Add("test2")
	if err != nil {
		suite.T().Fatalf("Failed to add to set %v", err)
	}
	err = set.Add("test3")
	if err != nil {
		suite.T().Fatalf("Failed to add to set %v", err)
	}
	err = set.Add("test4")
	if err != nil {
		suite.T().Fatalf("Failed to add to set %v", err)
	}
	err = set.Add("test4")
	if err == nil {
		suite.T().Fatalf("Expected adding an item twice to fail")
	}

	size, err := set.Size()
	if err != nil {
		suite.T().Fatalf("Failed to get size of set %v", err)
	}

	if size != 4 {
		suite.T().Fatalf("Expected set size to be 4 but was %d", size)
	}

	contains, err := set.Contains("test2")
	if err != nil {
		suite.T().Fatalf("Failed to get contains from set %v", err)
	}

	if !contains {
		suite.T().Fatalf("Expected contains to be true")
	}

	contains, err = set.Contains("test5")
	if err != nil {
		suite.T().Fatalf("Failed to get contains from set %v", err)
	}

	if contains {
		suite.T().Fatalf("Expected contains to be false")
	}

	iter, err := set.Iterator()
	if err != nil {
		suite.T().Fatalf("Failed to get iterator for set %v", err)
	}

	var items []string
	for _, item := range iter {
		items = append(items, item.(string))
	}

	expected := map[string]string{"test1": "", "test2": "", "test3": "", "test4": ""}
	for _, item := range items {
		_, ok := expected[item]
		if !ok {
			suite.T().Fatalf("Unexpected iterator entry %s", item)
		}
	}

	values, err := set.Values()
	if err != nil {
		suite.T().Fatalf("Failed to get values for set %v", err)
	}
	for _, item := range values {
		_, ok := expected[item.(string)]
		if !ok {
			suite.T().Fatalf("Unexpected values entry %s", item)
		}
	}

	err = set.Remove("test2")
	if err != nil {
		suite.T().Fatalf("Failed to remove from set %v", err)
	}

	size, err = set.Size()
	if err != nil {
		suite.T().Fatalf("Failed to get size of set %v", err)
	}

	if size != 3 {
		suite.T().Fatalf("Expected size after remove to be 3 but was %d", size)
	}

	iter2, err := set.Iterator()
	if err != nil {
		suite.T().Fatalf("Failed to get iterator for set %v", err)
	}

	var items2 []string
	for _, item := range iter2 {
		items2 = append(items2, item.(string))
	}

	expected2 := map[string]string{"test1": "", "test3": "", "test4": ""}
	for _, item := range items2 {
		_, ok := expected2[item]
		if !ok {
			suite.T().Fatalf("Unexpected iterator entry %s", item)
		}
	}

	err = set.Clear()
	if err != nil {
		suite.T().Fatalf("Failed to clear set %v", err)
	}
}

func (suite *IntegrationTestSuite) TestQueueCrud() {
	suite.skipIfUnsupported(KeyValueFeature)

	queue := globalCollection.Queue("testQueue")
	err := queue.Push("test1")
	if err != nil {
		suite.T().Fatalf("Failed to push to queue %v", err)
	}
	err = queue.Push("test2")
	if err != nil {
		suite.T().Fatalf("Failed to push to queue %v", err)
	}
	err = queue.Push("test3")
	if err != nil {
		suite.T().Fatalf("Failed to push to queue %v", err)
	}
	err = queue.Push("test4")
	if err != nil {
		suite.T().Fatalf("Failed to push to queue %v", err)
	}

	size, err := queue.Size()
	if err != nil {
		suite.T().Fatalf("Failed to get size of queue %v", err)
	}

	if size != 4 {
		suite.T().Fatalf("Expected queue size to be 4 but was %d", size)
	}

	iter, err := queue.Iterator()
	if err != nil {
		suite.T().Fatalf("Failed to get iterator for queue %v", err)
	}

	var items []string
	for _, item := range iter {
		items = append(items, item.(string))
	}

	expected := []string{"test4", "test3", "test2", "test1"}
	for i, item := range items {
		if expected[i] != item {
			suite.T().Fatalf("Expected item at %d to be %s but was %s", i, expected[i], item)
		}
	}

	var removed string
	err = queue.Pop(&removed)
	if err != nil {
		suite.T().Fatalf("Failed to remove from queue %v", err)
	}

	if removed != "test1" {
		suite.T().Fatalf("Expected test1 to be pop'd but was %s", removed)
	}

	size, err = queue.Size()
	if err != nil {
		suite.T().Fatalf("Failed to get size of queue %v", err)
	}

	if size != 3 {
		suite.T().Fatalf("Expected size after remove to be 3 but was %d", size)
	}

	iter2, err := queue.Iterator()
	if err != nil {
		suite.T().Fatalf("Failed to get iterator for queue %v", err)
	}

	var items2 []string
	for _, item := range iter2 {
		items2 = append(items2, item.(string))
	}

	expected2 := []string{"test4", "test3", "test2"}
	for i, item := range items2 {
		if expected2[i] != item {
			suite.T().Fatalf("Expected item at %d to be %s but was %s", i, expected2[i], item)
		}
	}

	err = queue.Clear()
	if err != nil {
		suite.T().Fatalf("Failed to clear queue %v", err)
	}
}

func (suite *IntegrationTestSuite) TestMapCrud() {
	suite.skipIfUnsupported(KeyValueFeature)

	cMap := globalCollection.Map("testMap")
	err := cMap.Add("test1", "test1val")
	if err != nil {
		suite.T().Fatalf("Failed to Add to cMap %v", err)
	}
	err = cMap.Add("test2", "test2val")
	if err != nil {
		suite.T().Fatalf("Failed to Add to cMap %v", err)
	}
	err = cMap.Add("test3", "test3val")
	if err != nil {
		suite.T().Fatalf("Failed to Add to cMap %v", err)
	}
	err = cMap.Add("test4", "test4val")
	if err != nil {
		suite.T().Fatalf("Failed to Add to cMap %v", err)
	}

	size, err := cMap.Size()
	if err != nil {
		suite.T().Fatalf("Failed to get size of cMap %v", err)
	}

	if size != 4 {
		suite.T().Fatalf("Expected cMap size to be 4 but was %d", size)
	}

	var test3Val string
	err = cMap.At("test3", &test3Val)
	suite.Require().Nil(err, err)
	suite.Assert().Equal("test3val", test3Val)

	exists, err := cMap.Exists("test3")
	suite.Require().Nil(err, err)

	suite.Assert().True(exists)

	iter, err := cMap.Iterator()
	if err != nil {
		suite.T().Fatalf("Failed to get iterator for cMap %v", err)
	}

	expected := map[string]interface{}{"test1": "test1val", "test2": "test2val", "test3": "test3val", "test4": "test4val"}
	for key, item := range iter {
		expectedItem, ok := expected[key]
		if !ok {
			suite.T().Fatalf("Expected map to contain %s but didn't", key)
		}
		if expectedItem != item {
			suite.T().Fatalf("Expected item at %s to be %s but was %s", key, expectedItem, item)
		}
	}

	err = cMap.Remove("test1")
	if err != nil {
		suite.T().Fatalf("Failed to remove from cMap %v", err)
	}

	suite.Assert().False(cMap.Exists("test1"))

	size, err = cMap.Size()
	if err != nil {
		suite.T().Fatalf("Failed to get size of cMap %v", err)
	}

	if size != 3 {
		suite.T().Fatalf("Expected size after remove to be 3 but was %d", size)
	}

	iter2, err := cMap.Iterator()
	if err != nil {
		suite.T().Fatalf("Failed to get iterator for cMap %v", err)
	}

	expected2 := map[string]interface{}{"test2": "test2val", "test3": "test3val", "test4": "test4val"}
	for key, item := range iter2 {
		expectedItem, ok := expected2[key]
		if !ok {
			suite.T().Fatalf("Expected map to contain %s but didn't", key)
		}
		if expectedItem != item {
			suite.T().Fatalf("Expected item at %s to be %s but was %s", key, expectedItem, item)
		}
	}

	err = cMap.Clear()
	if err != nil {
		suite.T().Fatalf("Failed to clear map %v", err)
	}
}
