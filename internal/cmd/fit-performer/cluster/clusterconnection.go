package cluster

import (
	"github.com/couchbase/gocb/v2"
)

type Connection struct {
	cluster *gocb.Cluster
	tracer  gocb.RequestTracer
	meter   gocb.Meter
}

func Connect(hostname string, opts gocb.ClusterOptions) (*Connection, error) {

	c, err := gocb.Connect(hostname, opts)
	if err != nil {
		return nil, err
	}

	conn := &Connection{
		cluster: c,
		tracer:  opts.Tracer,
		meter:   opts.Meter,
	}

	return conn, nil
}

func (c *Connection) Cluster() *gocb.Cluster {
	return c.cluster
}

func (c *Connection) DefaultBucket() *gocb.Bucket {
	return c.cluster.Bucket("default")
}

func (c *Connection) Bucket(name string) *gocb.Bucket {
	return c.cluster.Bucket(name)
}

func (c *Connection) Collection(bucket, scope, collection string) *gocb.Collection {
	return c.Bucket(bucket).Scope(scope).Collection(collection)
}

func (c *Connection) Tracer() gocb.RequestTracer {
	return c.tracer
}

func (c *Connection) Meter() gocb.Meter {
	return c.meter
}

func (c *Connection) Transactions() *gocb.Transactions {
	return c.cluster.Transactions()
}

func (c *Connection) Disconnect() error {
	err := c.cluster.Close(nil)
	c.cluster = nil
	return err
}
