package grpc

import "errors"

var (
	// ErrEmptyGigServiceAddress reports a missing gig service dial target.
	ErrEmptyGigServiceAddress = errors.New("gig service address is empty")
	// ErrNilLogger reports a missing logger dependency.
	ErrNilLogger = errors.New("logger is nil")
)
