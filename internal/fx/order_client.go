package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
	authgrpc "order-saga-service/internal/infra/auth/grpc"
	ordergrpc "order-saga-service/internal/infra/order/grpc"
	paymentgrpc "order-saga-service/internal/infra/payment/grpc"
)

var ClientModule = fx.Options(fx.Provide(ProvideAuthQueryClient), fx.Provide(ProvideOrderWriteClient), fx.Provide(ProvidePaymentCheckoutClient))

func ProvideAuthQueryClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.AuthQueryClient, error) {
	client, err := authgrpc.New(authgrpc.Config{Address: cfg.Auth.Address}, lg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return client.Close() }})
	return client, nil
}

func ProvideOrderWriteClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.OrderWriteClient, error) {
	client, err := ordergrpc.New(ordergrpc.Config{Address: cfg.Order.Address}, lg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return client.Close() }})
	return client, nil
}

func ProvidePaymentCheckoutClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.PaymentCheckoutClient, error) {
	client, err := paymentgrpc.New(paymentgrpc.Config{Address: cfg.Payment.Address}, lg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return client.Close() }})
	return client, nil
}
