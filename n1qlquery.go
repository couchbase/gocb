package gocb

type ConsistencyMode int

const (
	NotBounded    = ConsistencyMode(1)
	RequestPlus   = ConsistencyMode(2)
	StatementPlus = ConsistencyMode(3)
)

type N1qlQuery struct {
	options map[string]interface{}
}

func (nq *N1qlQuery) Consistency(stale ConsistencyMode) *N1qlQuery {
	if stale == NotBounded {
		nq.options["consistency"] = "not_bounded"
	} else if stale == RequestPlus {
		nq.options["consistency"] = "request_plus"
	} else if stale == StatementPlus {
		nq.options["consistency"] = "statement_plus"
	} else {
		panic("Unexpected consistency option")
	}
	return nq
}

func (nq *N1qlQuery) Custom(name, value string) *N1qlQuery {
	nq.options[name] = value
	return nq
}

func NewN1qlQuery(statement string) *N1qlQuery {
	nq := &N1qlQuery{
		options: make(map[string]interface{}),
	}
	nq.options["statement"] = statement
	return nq
}
