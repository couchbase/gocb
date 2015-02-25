package gocbcore

import (
	"strings"
)

type AuthClient interface {
	Address() string

	ExecSaslListMechs() ([]string, error)
	ExecSaslAuth(k, v []byte) ([]byte, error)
	ExecSaslStep(k, v []byte) ([]byte, error)
	ExecSelectBucket(b []byte) error
}

type authClient struct {
	pipeline *memdPipeline
}

func (client *authClient) Address() string {
	return client.pipeline.Address()
}

func (client *authClient) doBasicOp(cmd CommandCode, k, v []byte) ([]byte, error) {
	resp, err := client.pipeline.ExecuteRequest(&memdQRequest{
		memdRequest: memdRequest{
			Magic:  ReqMagic,
			Opcode: cmd,
			Key:    k,
			Value:  v,
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

func (client *authClient) ExecSaslListMechs() ([]string, error) {
	bytes, err := client.doBasicOp(CmdSASLListMechs, nil, nil)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(bytes), " "), nil
}

func (client *authClient) ExecSaslAuth(k, v []byte) ([]byte, error) {
	logDebugf("Performing SASL authentication. %s %v", k, v)
	return client.doBasicOp(CmdSASLAuth, k, v)
}

func (client *authClient) ExecSaslStep(k, v []byte) ([]byte, error) {
	return client.doBasicOp(CmdSASLStep, k, v)
}

func (client *authClient) ExecSelectBucket(b []byte) error {
	_, err := client.doBasicOp(CmdSelectBucket, nil, b)
	return err
}
