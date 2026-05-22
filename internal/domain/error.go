package domain

import "errors"

var (
	// ErrInvalidSessionID reports a missing or malformed order saga session id.
	ErrInvalidSessionID = errors.New("invalid session id")
	// ErrSessionNotFound reports a missing order saga session in persistence.
	ErrSessionNotFound = errors.New("order saga session not found")
	// ErrStepNotFound reports a missing order saga step in persistence.
	ErrStepNotFound = errors.New("order saga step not found")
)
