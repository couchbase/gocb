package couchbase

import "time"

type Cluster struct {
	manager *ClusterManager

	connSpecStr       string
	connectionTimeout time.Duration
}

func OpenCluster(connSpecStr string) (*Cluster, error) {
	cluster := &Cluster{
		connSpecStr:       connSpecStr,
		connectionTimeout: 10000 * time.Millisecond,
	}
	return cluster, nil
}

func (c *Cluster) OpenBucket(bucket, password string) (*Bucket, error) {
	cli, err := CreateCbIoRouter(c.connSpecStr, bucket, password)
	if err != nil {
		return nil, err
	}

	return &Bucket{
		client: cli,
	}, nil
}

func (c *Cluster) Manager(username, password string) *ClusterManager {
	if c.manager == nil {
		c.manager = &ClusterManager{}
	}
	return c.manager
}
