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
	ReleaseFunds(ctx context.Context, cmd app.ReleaseFundsCommand) (*app.ReleaseFundsResult, error)
	SettleDispute(ctx context.Context, cmd app.SettleDisputeCommand) (*app.SettleDisputeResult, error)
	GetReleaseByOrderID(ctx context.Context, orderID string) (*app.GetReleaseByOrderResult, error)
	Close() error
}

type Config struct {
	Address string `env:"ADDRESS,required"`
}
