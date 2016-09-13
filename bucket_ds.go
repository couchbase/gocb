package gocb

import "fmt"

// *UNCOMMITTED*
// MapGet retrieves a single item from a map document by its key.
func (b *Bucket) MapGet(key, path string, valuePtr interface{}) (Cas, error) {
	frag, err := b.LookupIn(key).Get(path).Execute()
	if err != nil {
		return 0, err
	}
	err = frag.ContentByIndex(0, valuePtr)
	if err != nil {
		return 0, err
	}
	return frag.Cas(), nil
}

// *UNCOMMITTED*
// MapRemove removes a specified key from the specified map document.
func (b *Bucket) MapRemove(key, path string) (Cas, error) {
	frag, err := b.MutateIn(key, 0, 0).Remove(path).Execute()
	if err != nil {
		return 0, err
	}
	return frag.Cas(), nil
}

// *UNCOMMITTED*
// MapSize returns the current number of items in a map document.
// PERFORMANCE NOTICE: This currently performs a full document fetch...
func (b *Bucket) MapSize(key string) (uint, Cas, error) {
	var mapContents map[string]interface{}
	cas, err := b.Get(key, &mapContents)
	if err != nil {
		return 0, 0, err
	}

	return uint(len(mapContents)), cas, nil
}

// *UNCOMMITTED*
// MapAdd inserts an item to a map document.
func (b *Bucket) MapAdd(key, path string, value interface{}, createMap bool) (Cas, error) {
	for {
		frag, err := b.MutateIn(key, 0, 0).Insert(path, value, false).Execute()
		if err != nil {
			if err == ErrKeyNotFound && createMap {
				data := make(map[string]interface{})
				data[path] = value
				cas, err := b.Insert(key, data, 0)
				if err != nil {
					if err == ErrKeyExists {
						continue
					}

					return 0, err
				}
				return cas, nil
			}
			return 0, err
		}
		return frag.Cas(), nil
	}
}

// *UNCOMMITTED*
// ListGet retrieves an item from a list document by index.
func (b *Bucket) ListGet(key string, index uint, valuePtr interface{}) (Cas, error) {
	frag, err := b.LookupIn(key).Get(fmt.Sprintf("[%d]", index)).Execute()
	if err != nil {
		return 0, err
	}
	err = frag.ContentByIndex(0, valuePtr)
	if err != nil {
		return 0, err
	}
	return frag.Cas(), nil
}

// *UNCOMMITTED*
// ListPush inserts an item to the end of a list document.
func (b *Bucket) ListPush(key string, value interface{}, createList bool) (Cas, error) {
	for {
		frag, err := b.MutateIn(key, 0, 0).ArrayAppend("", value, false).Execute()
		if err != nil {
			if err == ErrKeyNotFound && createList {
				var data []interface{}
				data = append(data, value)
				cas, err := b.Insert(key, data, 0)
				if err != nil {
					if err == ErrKeyExists {
						continue
					}

					return 0, err
				}
				return cas, nil
			}
			return 0, err
		}
		return frag.Cas(), nil
	}
}

// *UNCOMMITTED*
// ListShift inserts an item to the beginning of a list document.
func (b *Bucket) ListShift(key string, value interface{}, createList bool) (Cas, error) {
	for {
		frag, err := b.MutateIn(key, 0, 0).ArrayPrepend("", value, false).Execute()
		if err != nil {
			if err == ErrKeyNotFound && createList {
				var data []interface{}
				data = append(data, value)
				cas, err := b.Insert(key, data, 0)
				if err != nil {
					if err == ErrKeyExists {
						continue
					}

					return 0, err
				}
				return cas, nil
			}
			return 0, err
		}
		return frag.Cas(), nil
	}
}

// *UNCOMMITTED*
// ListRemove removes an item from a list document by its index.
func (b *Bucket) ListRemove(key string, index uint) (Cas, error) {
	frag, err := b.MutateIn(key, 0, 0).Remove(fmt.Sprintf("[%d]", index)).Execute()
	if err != nil {
		return 0, err
	}
	return frag.Cas(), nil
}

// *UNCOMMITTED*
// ListSet replaces the item at a particular index of a list document.
func (b *Bucket) ListSet(key string, index uint, value interface{}) (Cas, error) {
	frag, err := b.MutateIn(key, 0, 0).Replace(fmt.Sprintf("[%d]", index), value).Execute()
	if err != nil {
		return 0, err
	}
	return frag.Cas(), nil
}

// *UNCOMMITTED*
// MapSize returns the current number of items in a list.
// PERFORMANCE NOTICE: This currently performs a full document fetch...
func (b *Bucket) ListSize(key string) (uint, Cas, error) {
	var listContents []interface{}
	cas, err := b.Get(key, &listContents)
	if err != nil {
		return 0, 0, err
	}

	return uint(len(listContents)), cas, nil
}

// *UNCOMMITTED*
// SetAdd adds a new value to a set document.
func (b *Bucket) SetAdd(key string, value interface{}, createSet bool) (Cas, error) {
	for {
		frag, err := b.MutateIn(key, 0, 0).ArrayAddUnique("", value, false).Execute()
		if err != nil {
			if err == ErrKeyNotFound && createSet {
				var data []interface{}
				data = append(data, value)
				cas, err := b.Insert(key, data, 0)
				if err != nil {
					if err == ErrKeyExists {
						continue
					}

					return 0, err
				}
				return cas, nil
			}
			return 0, err
		}
		return frag.Cas(), nil
	}
}

// *UNCOMMITTED*
// SetExists checks if a particular value exists within the specified set document.
// PERFORMANCE WARNING: This performs a full set fetch and compare.
func (b *Bucket) SetExists(key string, value interface{}) (bool, Cas, error) {
	var setContents []interface{}
	cas, err := b.Get(key, &setContents)
	if err != nil {
		return false, 0, err
	}

	for _, item := range setContents {
		if item == value {
			return true, cas, nil
		}
	}

	return false, 0, nil
}

// *UNCOMMITTED*
// MapSize returns the current number of values in a set.
// PERFORMANCE NOTICE: This currently performs a full document fetch...
func (b *Bucket) SetSize(key string) (uint, Cas, error) {
	var setContents []interface{}
	cas, err := b.Get(key, &setContents)
	if err != nil {
		return 0, 0, err
	}

	return uint(len(setContents)), cas, nil
}

// *UNCOMMITTED*
// SetRemove removes a specified value from the specified set document.
// WARNING: This relies on Go's interface{} comparison behaviour!
// PERFORMANCE WARNING: This performs full set fetch, modify, store cycles.
func (b *Bucket) SetRemove(key string, value interface{}) (Cas, error) {
	for {
		var setContents []interface{}
		cas, err := b.Get(key, &setContents)
		if err != nil {
			return 0, err
		}

		foundItem := false
		var newSetContents []interface{}
		for _, item := range setContents {
			if item == value {
				foundItem = true
			} else {
				newSetContents = append(newSetContents, value)
			}
		}

		if !foundItem {
			return 0, ErrRangeError
		}

		b.Replace(key, newSetContents, cas, 0)
		if err != nil {
			if err == ErrKeyExists {
				// If this is just a CAS error, try again!
				continue
			}

			return 0, err
		}

		return cas, nil
	}
}
