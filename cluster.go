package couchbase

import "time"

type Cluster struct {
	manager *ClusterManager

	connSpecStr       string
	connectionTimeout time.Duration
}

func OpenCluster(connSpecStr string) (*Cluster, Error) {
	cluster := &Cluster{
		connSpecStr:       connSpecStr,
		connectionTimeout: 10000 * time.Millisecond,
	}
	return cluster, nil
}

func (c *Cluster) OpenBucket(bucket, password string) (*Bucket, Error) {
	cli, err := CreateBaseConnection(c.connSpecStr, bucket, password)
	if err != nil {
		return nil, err
	}

	return &Bucket{
		Client: cli,
	}, nil
}

func (c *Cluster) Manager(username, password string) *ClusterManager {
	if c.manager == nil {
		c.manager = &ClusterManager{}
	}
	return c.manager
}
