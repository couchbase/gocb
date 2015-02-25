package gocb

type clientError struct {
	message string
}

func (e clientError) Error() string {
	return e.message
}

type timeoutError struct {
}

func (e timeoutError) Error() string {
	return "The operation has timed out."
}
func (e timeoutError) Timeout() bool {
	return true
}
