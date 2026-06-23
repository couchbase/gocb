package txnerrors

import "errors"

var ErrTestFailed = errors.New("test failed")
var ErrInternal = errors.New("internal performer error")
