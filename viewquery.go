package couchbase

import "strconv"
import "encoding/json"

type StaleMode int

const (
	BEFORE = StaleMode(1)
	NONE   = StaleMode(2)
	AFTER  = StaleMode(3)
)

type SortOrder int

const (
	ASCENDING  = SortOrder(1)
	DESCENDING = SortOrder(2)
)

type ViewQuery struct {
	ddoc    string
	name    string
	options map[string]string
}

func (vq *ViewQuery) from(ddoc, name string) *ViewQuery {
	vq.ddoc = ddoc
	vq.name = name
	return vq
}

func (vq *ViewQuery) Stale(stale StaleMode) *ViewQuery {
	if stale == BEFORE {
		vq.options["stale"] = "false"
	} else if stale == NONE {
		vq.options["stale"] = "ok"
	} else if stale == AFTER {
		vq.options["stale"] = "update_after"
	} else {
		panic("Unexpected stale option")
	}
	return vq
}
func (vq *ViewQuery) UpdateBefore() *ViewQuery {
	vq.Stale(BEFORE)
	return vq
}
func (vq *ViewQuery) UpdateNone() *ViewQuery {
	vq.Stale(NONE)
	return vq
}
func (vq *ViewQuery) UpdateAfter() *ViewQuery {
	vq.Stale(AFTER)
	return vq
}

func (vq *ViewQuery) Skip(num uint) *ViewQuery {
	vq.options["skip"] = strconv.FormatUint(uint64(num), 10)
	return vq
}

func (vq *ViewQuery) Limit(num uint) *ViewQuery {
	vq.options["limit"] = strconv.FormatUint(uint64(num), 10)
	return vq
}

func (vq *ViewQuery) Order(order SortOrder) *ViewQuery {
	if order == ASCENDING {
		vq.options["descending"] = "false"
	} else if order == DESCENDING {
		vq.options["descending"] = "true"
	} else {
		panic("Unexpected order option")
	}
	return vq
}

func (vq *ViewQuery) Reduce(reduce bool) *ViewQuery {
	if reduce == true {
		vq.options["reduce"] = "true"
	} else {
		vq.options["reduce"] = "false"
	}
	return vq
}

func (vq *ViewQuery) Group(level int) *ViewQuery {
	if level >= 0 {
		vq.options["group"] = "false"
		vq.options["group_level"] = strconv.FormatInt(int64(level), 10)
	} else {
		vq.options["group"] = "true"
		vq.options["group_level"] = "0"
	}
	return vq
}

func (vq *ViewQuery) Key(key string) *ViewQuery {
	jsonKey, _ := json.Marshal(key)
	vq.options["key"] = string(jsonKey)
	return vq
}

func (vq *ViewQuery) Keys(keys []string) *ViewQuery {
	jsonKeys, _ := json.Marshal(keys)
	vq.options["keys"] = string(jsonKeys)
	return vq
}

//func (vq *ViewQuery) Range(start, end ??, inclusive_end bool) *ViewQuery {
//}

//func (vq *ViewQuery) IdRange(start, end ??) *ViewQuery {
//}

func (vq *ViewQuery) Custom(name, value string) *ViewQuery {
	vq.options[name] = value
	return vq
}

func NewViewQuery(ddoc, name string) *ViewQuery {
	return new(ViewQuery).from(ddoc, name)
}
