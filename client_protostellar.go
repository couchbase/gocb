package gocb

// type protoStellarConnectionMgr struct {
// 	host   string
// 	lock   sync.Mutex
// 	config *gocbcoreps.DialOptions
// 	agent  *gocbcoreps.RoutingClient
// }
//
// func (c *protoStellarConnectionMgr) connect() error {
// 	c.lock.Lock()
// 	defer c.lock.Unlock()
//
// 	client, err := gocbcoreps.Dial(c.host, c.config)
// 	if err != nil {
// 		return err
// 	}
//
// 	c.agent = client
//
// 	return nil
// }
//
// func (c *protoStellarConnectionMgr) openBucket(bucketName string) error {
// 	c.agent.OpenBucket(bucketName)
// 	return nil
// }
//
// func (c *protoStellarConnectionMgr) buildConfig(cluster *Cluster) error {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getKvProvider(bucketName string) (kvProvider, error) {
// 	kv := c.agent.KvV1()
// 	return &protoStellarKVProvider{}, errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getKvCapabilitiesProvider(bucketName string) (kvCapabilityVerifier, error) {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getViewProvider(bucketName string) (viewProvider, error) {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getQueryProvider() (queryProvider, error) {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getAnalyticsProvider() (analyticsProvider, error) {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getSearchProvider() (searchProvider, error) {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getHTTPProvider(bucketName string) (httpProvider, error) {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getDiagnosticsProvider(bucketName string) (diagnosticsProvider, error) {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) getWaitUntilReadyProvider(bucketName string) (waitUntilReadyProvider, error) {
// 	return errors.New("not implemented")
// }
// func (c *protoStellarConnectionMgr) connection(bucketName string) (*gocbcore.Agent, error) {
// 	return nil, errors.New("is not implemented")
// }
// func (c *protoStellarConnectionMgr) close() error {
// 	return errors.New("not implemented")
// }
