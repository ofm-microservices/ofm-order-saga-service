package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	app "order-saga-service/internal/application"
)

type Logger = logging.Logger

type Client interface {
	GetConnectStatus(ctx context.Context, userID string) (*app.GetConnectStatusResult, error)
	CreateCheckoutSession(ctx context.Context, cmd app.CreateCheckoutSessionCommand) (*app.CreateCheckoutSessionResult, error)
	Close() error
}

type Config struct {
	Address string `env:"ADDRESS,required"`
}
