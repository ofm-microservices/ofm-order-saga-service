package grpc

import "errors"

var (
	// ErrEmptyAddress reports a missing auth service address.
	ErrEmptyAddress = errors.New("auth service address is empty")
	// ErrNilLogger reports a missing logger dependency.
	ErrNilLogger = errors.New("logger is nil")
)
