package helpers

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/counter"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"github.com/google/uuid"
)

func Location(location *shared.DocLocation, counters *counter.Counters) (*DocLocation, error) {
	switch loc := location.Location.(type) {
	case *shared.DocLocation_Specific:
		return &DocLocation{
			collection: loc.Specific.Collection,
			id:         loc.Specific.Id,
		}, nil
	case *shared.DocLocation_Pool:
		var next int
		switch strat := loc.Pool.PoolSelectionStrategy.(type) {
		case *shared.DocLocationPool_Random:
			if strat.Random.Distribution == shared.RandomDistribution_RANDOM_DISTRIBUTION_UNIFORM {
				next = rand.Intn(int(loc.Pool.PoolSize)) //nolint:gosec
			} else {
				return nil, errors.New("unrecognised random distribution")
			}
		case *shared.DocLocationPool_Counter:
			c, err := counters.Get(strat.Counter.Counter)
			if err != nil {
				return nil, err
			}

			next = int(c.GetAndIncrement()) % int(loc.Pool.PoolSize)
		default:
			return nil, errors.New("unrecognised pool selection strategy")
		}

		return &DocLocation{
			collection: loc.Pool.Collection,
			id:         fmt.Sprintf("%s%d", loc.Pool.IdPreface, next),
		}, nil
	case *shared.DocLocation_Uuid:
		return &DocLocation{
			collection: loc.Uuid.Collection,
			id:         uuid.NewString(),
		}, nil
	default:
		return nil, errors.New("command had no valid location")
	}
}

type DocLocation struct {
	collection *shared.Collection
	id         string
}

func (loc *DocLocation) ID() string {
	return loc.id
}

func (loc *DocLocation) Bucket() string {
	return loc.collection.BucketName
}

func (loc *DocLocation) Scope() string {
	return loc.collection.ScopeName
}

func (loc *DocLocation) Collection() string {
	return loc.collection.CollectionName
}
