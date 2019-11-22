package gocb

// Collection represents a single collection.
type Collection struct {
	sb stateBlock
}

func newCollection(scope *Scope, collectionName string) *Collection {
	collection := &Collection{
		sb: scope.stateBlock(),
	}
	collection.sb.CollectionName = collectionName

	return collection
}

func (c *Collection) name() string {
	return c.sb.CollectionName
}

func (c *Collection) scopeName() string {
	return c.sb.ScopeName
}

func (c *Collection) clone() *Collection {
	newC := *c
	return &newC
}

func (c *Collection) getKvProvider() (kvProvider, error) {
	cli := c.sb.getCachedClient()
	agent, err := cli.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent, nil
}

// Name returns the name of the collection.
func (c *Collection) Name() string {
	return c.sb.CollectionName
}

func (c *Collection) startKvOpTrace(operationName string, tracectx requestSpanContext) requestSpan {
	return c.sb.Tracer.StartSpan(operationName, tracectx).
		SetTag("couchbase.bucket", c.sb.BucketName).
		SetTag("couchbase.collection", c.sb.CollectionName).
		SetTag("couchbase.service", "kv")
}
