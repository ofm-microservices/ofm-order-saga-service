package grpc

import "errors"

var (
	ErrEmptyAddress = errors.New("payment service address is empty")
	ErrNilLogger    = errors.New("logger is nil")
)
