package grpc

import app "order-saga-service/internal/application"

// Config holds the outbound auth-service address.
type Config struct {
	Address string
}

// Client is the outbound auth-query gRPC boundary used by the saga.
type Client interface {
	app.AuthQueryClient
}
