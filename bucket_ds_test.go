package gocb

import (
	"testing"
)

func TestDsMapGetAndSize(t *testing.T) {
	itemMap := make(map[string]string)
	itemMap["one"] = "1"
	itemMap["two"] = "2"
	itemMap["three"] = "3"
	itemMap["four"] = "4"

	_, err := globalBucket.Upsert("mapGet", itemMap, 0)
	if err != nil {
		t.Fatalf("Failed to setup map document %v", err)
	}

	var mapValue string
	_, err = globalBucket.MapGet("mapGet", "three", &mapValue)
	if err != nil {
		t.Fatalf("Failed to retrieve map item contents %v", err)
	}
	if mapValue != "3" {
		t.Fatalf("MapGet failed to retrieve the correct item value")
	}

	size, _, err := globalBucket.MapSize("mapGet")
	if err != nil {
		t.Fatalf("Failed to retrieve map size %v", err)
	}
	if size != 4 {
		t.Fatalf("MapSize failed to get correct size")
	}
}

func TestDsMapRemove(t *testing.T) {
	itemMap := make(map[string]string)
	itemMap["one"] = "1"
	itemMap["two"] = "2"
	itemMap["three"] = "3"
	itemMap["four"] = "4"

	_, err := globalBucket.Upsert("mapRemove", itemMap, 0)
	if err != nil {
		t.Fatalf("Failed to setup map document %v", err)
	}

	_, err = globalBucket.MapRemove("mapRemove", "two")
	if err != nil {
		t.Fatalf("Failed to remove map item %v", err)
	}

	var mapContents map[string]string
	_, err = globalBucket.Get("mapRemove", &mapContents)
	if err != nil {
		t.Fatalf("Failed to retrieve map contents %v", err)
	}
	if _, found := mapContents["two"]; len(mapContents) != 3 || found {
		t.Fatalf("Failed to remove the item")
	}
}

func TestDsMapAdd(t *testing.T) {
	itemMap := make(map[string]string)
	itemMap["one"] = "1"
	itemMap["two"] = "2"

	_, err := globalBucket.Upsert("mapAdd", itemMap, 0)
	if err != nil {
		t.Fatalf("Failed to setup map document %v", err)
	}

	_, err = globalBucket.MapAdd("mapAdd", "three", "3", false)
	if err != nil {
		t.Fatalf("Failed to remove map item %v", err)
	}

	var mapContents map[string]string
	_, err = globalBucket.Get("mapAdd", &mapContents)
	if err != nil {
		t.Fatalf("Failed to retrieve map contents %v", err)
	}
	if _, found := mapContents["three"]; !found ||
		len(mapContents) != 3 || mapContents["three"] != "3" {
		t.Fatalf("Failed to add the item")
	}
}

func TestDsListGet(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "two")
	itemArray = append(itemArray, "three")
	itemArray = append(itemArray, "four")

	_, err := globalBucket.Upsert("listGet", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup list document %v", err)
	}

	var listValue0 string
	_, err = globalBucket.ListGet("listGet", 1, &listValue0)
	if err != nil {
		t.Fatalf("Failed to retrieve from list %v", err)
	}
	if listValue0 != "two" {
		t.Fatalf("Failed to retrieve correct data from list")
	}

	size, _, err := globalBucket.ListSize("listGet")
	if err != nil {
		t.Fatalf("Failed to retrieve list size %v", err)
	}
	if size != 4 {
		t.Fatalf("ListSize failed to get correct size")
	}
}

func TestDsListAppendAndPrepend(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")

	_, err := globalBucket.Upsert("listAppendPrepend", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup list document %v", err)
	}

	_, err = globalBucket.ListAppend("listAppendPrepend", "two", false)
	if err != nil {
		t.Fatalf("Failed to append to list %v", err)
	}

	_, err = globalBucket.ListPrepend("listAppendPrepend", "three", false)
	if err != nil {
		t.Fatalf("Failed to prepend to list %v", err)
	}

	var listContents []string
	_, err = globalBucket.Get("listAppendPrepend", &listContents)
	if err != nil {
		t.Fatalf("Failed to retrieve list contents %v", err)
	}
	if len(listContents) != 3 || listContents[0] != "three" ||
		listContents[2] != "two" {
		t.Fatalf("ListAppend failed to push the item")
	}
}

func TestDsListRemove(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "two")
	itemArray = append(itemArray, "three")
	itemArray = append(itemArray, "four")

	_, err := globalBucket.Upsert("listRemove", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup list document %v", err)
	}

	_, err = globalBucket.ListRemove("listRemove", 2)
	if err != nil {
		t.Fatalf("Failed to remove from list %v", err)
	}

	var listContents []string
	_, err = globalBucket.Get("listRemove", &listContents)
	if err != nil {
		t.Fatalf("Failed to retrieve list contents %v", err)
	}
	if len(listContents) != 3 {
		t.Fatalf("ListRemove failed to remove the item")
	}
}

func TestDsListSet(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "two")
	itemArray = append(itemArray, "three")

	_, err := globalBucket.Upsert("listSet", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup list document %v", err)
	}

	_, err = globalBucket.ListSet("listSet", 2, "six")
	if err != nil {
		t.Fatalf("Failed to set list item %v", err)
	}

	var listContents []string
	_, err = globalBucket.Get("listSet", &listContents)
	if err != nil {
		t.Fatalf("Failed to retrieve list contents %v", err)
	}
	if listContents[2] != "six" {
		t.Fatalf("ListSet failed to set the item")
	}
}

func TestDsSetAdd(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")

	_, err := globalBucket.Upsert("setAdd", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup set document %v", err)
	}

	_, err = globalBucket.SetAdd("setAdd", "four", false)
	if err != nil {
		t.Fatalf("Failed to perform set add %v", err)
	}

	var setContents []string
	_, err = globalBucket.Get("setAdd", &setContents)
	if err != nil {
		t.Fatalf("Failed to retrieve set contents %v", err)
	}
	if len(setContents) != 2 {
		t.Fatalf("SetAdd failed to add the item")
	}
}

func TestDsSetExists(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "two")
	itemArray = append(itemArray, "three")

	_, err := globalBucket.Upsert("setExists", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup set document %v", err)
	}

	exists, _, err := globalBucket.SetExists("setExists", "three")
	if err != nil {
		t.Fatalf("Failed to perform set exists check %v", err)
	}

	if !exists {
		t.Fatalf("Failed to accurately detect existing value")
	}

	exists, _, err = globalBucket.SetExists("setExists", "five")
	if err != nil {
		t.Fatalf("Failed to perform unset exists check %v", err)
	}

	if exists {
		t.Fatalf("Failed to accurately detect missing value")
	}
}

func TestDsSetRemove(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "two")
	itemArray = append(itemArray, "three")
	itemArray = append(itemArray, "four")

	_, err := globalBucket.Upsert("setRemove", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup set document %v", err)
	}

	_, err = globalBucket.SetRemove("setRemove", "three")
	if err != nil {
		t.Fatalf("Failed to remove from set %v", err)
	}

	var setContents []string
	_, err = globalBucket.Get("setRemove", &setContents)
	if err != nil {
		t.Fatalf("Failed to retrieve set contents %v", err)
	}

	if len(setContents) != 3 {
		t.Fatalf("SetRemove failed to remove the item")
	}

	// Check to ensure the correct items are in the set
	if setContents[0] != "one" {
		t.Fatalf("SetRemove removed the incorrect item")
	}
	if setContents[1] != "two" {
		t.Fatalf("SetRemove removed the incorrect item")
	}
	if setContents[2] != "four" {
		t.Fatalf("SetRemove removed the incorrect item")
	}
}

func TestDsSetSize(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "two")
	itemArray = append(itemArray, "three")

	_, err := globalBucket.Upsert("setSize", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup set document %v", err)
	}

	size, _, err := globalBucket.SetSize("setSize")
	if err != nil {
		t.Fatalf("Failed to perform set size op %v", err)
	}

	if size != 3 {
		t.Fatalf("SetSize failed to return accurate size")
	}
}

func TestDsQueuePush(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "two")

	_, err := globalBucket.Upsert("queuePush", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup queue document %v", err)
	}

	_, err = globalBucket.QueuePush("queuePush", "three", false)
	if err != nil {
		t.Fatalf("Failed to perform queue push %v", err)
	}

	var queueContents []string
	_, err = globalBucket.Get("queuePush", &queueContents)
	if err != nil {
		t.Fatalf("Failed to retrieve queue contents %v", err)
	}
	if len(queueContents) != 3 {
		t.Fatalf("QueuePush failed to push the item")
	}
}

func TestDsQueuePop(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "three")
	itemArray = append(itemArray, "two")

	_, err := globalBucket.Upsert("queuePop", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup queue document %v", err)
	}

	var queueItemValue string
	_, err = globalBucket.QueuePop("queuePop", &queueItemValue)
	if err != nil {
		t.Fatalf("Failed to perform queue pop %v", err)
	}

	if queueItemValue != "two" {
		t.Fatalf("Failed to retrieve the correct queue item")
	}

	var queueContents []string
	_, err = globalBucket.Get("queuePop", &queueContents)
	if err != nil {
		t.Fatalf("Failed to retrieve queue contents %v", err)
	}
	if len(queueContents) != 2 {
		t.Fatalf("QueuePop failed to remove the item")
	}
}

func TestDsQueueSize(t *testing.T) {
	var itemArray []string
	itemArray = append(itemArray, "one")
	itemArray = append(itemArray, "two")
	itemArray = append(itemArray, "three")

	_, err := globalBucket.Upsert("queueSize", itemArray, 0)
	if err != nil {
		t.Fatalf("Failed to setup queue document %v", err)
	}

	size, _, err := globalBucket.QueueSize("queueSize")
	if err != nil {
		t.Fatalf("Failed to perform queue size op %v", err)
	}

	if size != 3 {
		t.Fatalf("QueueSize failed to return accurate size")
	}
}
