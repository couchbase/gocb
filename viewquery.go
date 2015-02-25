package gocb

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
)

type StaleMode int

const (
	Before = StaleMode(1)
	None   = StaleMode(2)
	After  = StaleMode(3)
)

type SortOrder int

const (
	Ascending  = SortOrder(1)
	Descending = SortOrder(2)
)

type ViewQuery struct {
	ddoc    string
	name    string
	options url.Values
}

func (vq *ViewQuery) Stale(stale StaleMode) *ViewQuery {
	if stale == Before {
		vq.options.Set("stale", "false")
	} else if stale == None {
		vq.options.Set("stale", "ok")
	} else if stale == After {
		vq.options.Set("stale", "update_after")
	} else {
		panic("Unexpected stale option")
	}
	return vq
}

func (vq *ViewQuery) Skip(num uint) *ViewQuery {
	vq.options.Set("skip", strconv.FormatUint(uint64(num), 10))
	return vq
}

func (vq *ViewQuery) Limit(num uint) *ViewQuery {
	vq.options.Set("limit", strconv.FormatUint(uint64(num), 10))
	return vq
}

func (vq *ViewQuery) Order(order SortOrder) *ViewQuery {
	if order == Ascending {
		vq.options.Set("descending", "false")
	} else if order == Descending {
		vq.options.Set("descending", "true")
	} else {
		panic("Unexpected order option")
	}
	return vq
}

func (vq *ViewQuery) Reduce(reduce bool) *ViewQuery {
	if reduce == true {
		vq.options.Set("reduce", "true")
	} else {
		vq.options.Set("reduce", "false")
	}
	return vq
}

func (vq *ViewQuery) Group(useGrouping bool) *ViewQuery {
	if useGrouping {
		vq.options.Set("group", "true")
	} else {
		vq.options.Set("group", "false")
	}
	return vq
}

func (vq *ViewQuery) GroupLevel(groupLevel uint) *ViewQuery {
	vq.options.Set("group_level", strconv.FormatUint(uint64(groupLevel), 10))
	return vq
}

func (vq *ViewQuery) Key(key interface{}) *ViewQuery {
	jsonKey, _ := json.Marshal(key)
	vq.options.Set("key", string(jsonKey))
	return vq
}

func (vq *ViewQuery) Keys(keys []interface{}) *ViewQuery {
	jsonKeys, _ := json.Marshal(keys)
	vq.options.Set("keys", string(jsonKeys))
	return vq
}

func (vq *ViewQuery) Range(start, end interface{}, inclusive_end bool) *ViewQuery {
	// TODO(brett19): Not currently handling errors due to no way to return the error
	if start != nil {
		jsonStartKey, _ := json.Marshal(start)
		vq.options.Set("startkey", string(jsonStartKey))
	} else {
		vq.options.Del("startkey")
	}
	if end != nil {
		jsonEndKey, _ := json.Marshal(end)
		vq.options.Set("endkey", string(jsonEndKey))
	} else {
		vq.options.Del("endkey")
	}
	if start != nil || end != nil {
		if inclusive_end {
			vq.options.Set("inclusive_end", "true")
		} else {
			vq.options.Set("inclusive_end", "false")
		}
	} else {
		vq.options.Del("inclusive_end")
	}
	return vq
}

func (vq *ViewQuery) IdRange(start, end string) *ViewQuery {
	if start != "" {
		vq.options.Set("startkey_docid", start)
	} else {
		vq.options.Del("startkey_docid")
	}
	if end != "" {
		vq.options.Set("endkey_docid", end)
	} else {
		vq.options.Del("endkey_docid")
	}
	return vq
}

func (vq *ViewQuery) Development(val bool) *ViewQuery {
	if val {
		if !strings.HasPrefix(vq.ddoc, "dev_") {
			vq.ddoc = "dev_" + vq.ddoc
		}
	} else {
		vq.ddoc = strings.TrimPrefix(vq.ddoc, "dev_")
	}
	return vq
}

func (vq *ViewQuery) Custom(name, value string) *ViewQuery {
	vq.options.Set(name, value)
	return vq
}

func NewViewQuery(ddoc, name string) *ViewQuery {
	return &ViewQuery{
		ddoc:    ddoc,
		name:    name,
		options: url.Values{},
	}
}
