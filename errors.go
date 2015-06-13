package gocb

type clientError struct {
	message string
}

func (e clientError) Error() string {
	return e.message
}

func (e clientError) ClientError() bool {
	return true
}

type timeoutError struct {
}

func (e timeoutError) Error() string {
	return "The operation has timed out."
}

func (e timeoutError) Timeout() bool {
	return true
}
