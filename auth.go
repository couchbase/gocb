package gocb

type Authenticator interface {
	clusterMgmt() userPassPair
	clusterN1ql() []userPassPair
	bucketMemd(bucket string) string
	bucketMgmt(bucket string) userPassPair
	bucketViews(bucket string) userPassPair
	bucketN1ql(bucket string) []userPassPair
}

type BucketAuthenticator struct {
	Password string
}

type userPassPair struct {
	Username string `json:"user"`
	Password string `json:"pass"`
}

type BucketAuthenticatorMap map[string]BucketAuthenticator

type ClusterAuthenticator struct {
	Buckets  BucketAuthenticatorMap
	Username string
	Password string
}

func (ca ClusterAuthenticator) clusterMgmt() userPassPair {
	return userPassPair{ca.Username, ca.Password}
}

func (ca ClusterAuthenticator) clusterN1ql() []userPassPair {
	userPassList := make([]userPassPair, 0)
	for bucket, auth := range ca.Buckets {
		userPassList = append(userPassList, userPassPair{
			Username: bucket,
			Password: auth.Password,
		})
	}
	return userPassList
}

func (ca ClusterAuthenticator) bucketAll(bucket string) userPassPair {
	if bucketAuth, ok := ca.Buckets[bucket]; ok {
		return userPassPair{bucket, bucketAuth.Password}
	}
	return userPassPair{"", ""}
}

func (ca ClusterAuthenticator) bucketMemd(bucket string) string {
	return ca.bucketAll(bucket).Password
}

func (ca ClusterAuthenticator) bucketMgmt(bucket string) userPassPair {
	return ca.bucketAll(bucket)
}

func (ca ClusterAuthenticator) bucketViews(bucket string) userPassPair {
	return ca.bucketAll(bucket)
}

func (ca ClusterAuthenticator) bucketN1ql(bucket string) []userPassPair {
	return []userPassPair{
		ca.bucketAll(bucket),
	}
}
