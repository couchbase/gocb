package sender

import "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"

type ResultSender interface {
	Send(*run.Result)
}
