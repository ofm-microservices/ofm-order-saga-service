package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	app "order-saga-service/internal/application"
)

// Logger aliases the shared structured logger used by the client adapter.
type Logger = logging.Logger

// Config carries the gig-service gRPC target.
type Config struct {
	Address string
}

// Client resolves the published gig snapshot required by the order saga.
type Client interface {
	GetOrderStartSnapshot(ctx context.Context, gigID, packageID string) (*app.OrderStartSnapshot, error)
	Close() error
}
