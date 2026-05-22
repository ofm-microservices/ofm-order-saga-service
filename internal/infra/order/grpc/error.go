package grpc

import "errors"

var (
	ErrEmptyAddress = errors.New("order service address is empty")
	ErrNilLogger    = errors.New("logger is nil")
)
