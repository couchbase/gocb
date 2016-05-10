package gocb

import (
	"time"
)

type ConsistencyMode int

const (
	NotBounded    = ConsistencyMode(1)
	RequestPlus   = ConsistencyMode(2)
	StatementPlus = ConsistencyMode(3)
)

type N1qlQuery struct {
	options map[string]interface{}
	adHoc   bool
}

func (nq *N1qlQuery) Consistency(stale ConsistencyMode) *N1qlQuery {
	if _, ok := nq.options["scan_vectors"]; ok {
		panic("Consistent and ConsistentWith must be used exclusively")
	}
	if stale == NotBounded {
		nq.options["scan_consistency"] = "not_bounded"
	} else if stale == RequestPlus {
		nq.options["scan_consistency"] = "request_plus"
	} else if stale == StatementPlus {
		nq.options["scan_consistency"] = "statement_plus"
	} else {
		panic("Unexpected consistency option")
	}
	return nq
}

func (nq *N1qlQuery) ConsistentWith(state *MutationState) *N1qlQuery {
	if _, ok := nq.options["scan_consistency"]; ok {
		panic("Consistent and ConsistentWith must be used exclusively")
	}
	nq.options["scan_consistency"] = "at_plus"
	nq.options["scan_vectors"] = state
	return nq
}

func (nq *N1qlQuery) AdHoc(adhoc bool) *N1qlQuery {
	nq.adHoc = adhoc
	return nq
}

func (nq *N1qlQuery) Custom(name, value string) *N1qlQuery {
	nq.options[name] = value
	return nq
}

func (nq *N1qlQuery) Timeout(timeout time.Duration) *N1qlQuery {
	nq.options["timeout"] = timeout.String()
	return nq
}

func NewN1qlQuery(statement string) *N1qlQuery {
	nq := &N1qlQuery{
		options: make(map[string]interface{}),
		adHoc:   true,
	}
	nq.options["statement"] = statement
	return nq
}
