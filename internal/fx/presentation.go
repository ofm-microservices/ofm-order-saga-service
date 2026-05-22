package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
	events "order-saga-service/internal/presentation/event_broker/nats"
	grpcsrv "order-saga-service/internal/presentation/grpc"

	"go.uber.org/fx"
)

// PresentationModule wires the message consumers.
var PresentationModule = fx.Options(
	fx.Provide(ProvideResultSubscriber),
	fx.Provide(ProvideStartSubscriber),
	fx.Provide(ProvideGRPCServer),
	fx.Invoke(InvokeSubscribeResults),
	fx.Invoke(InvokeSubscribeStart),
	fx.Invoke(InvokeRunGRPCServer),
)

// ProvideResultSubscriber constructs the saga result subscriber.
func ProvideResultSubscriber(broker app.EventBroker, svc app.Service, cfg *config.Config, lg logging.Logger) *events.ResultSubscriber {
	return events.NewResultSubscriber(broker, svc, cfg.NATS, lg)
}

// ProvideStartSubscriber constructs the order start subscriber.
func ProvideStartSubscriber(broker app.EventBroker, svc app.Service, cfg *config.Config, lg logging.Logger) *events.StartSubscriber {
	return events.NewStartSubscriber(broker, svc, cfg.NATS, lg)
}

// InvokeSubscribeResults starts the NATS subscriptions.
func InvokeSubscribeResults(lc fx.Lifecycle, sub *events.ResultSubscriber, lg logging.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			if err := sub.Subscribe(context.Background()); err != nil {
				lg.Error("subscribe order saga results failed", logging.Err(err))
				return err
			}
			return nil
		},
	})
}

// InvokeSubscribeStart starts the order start subscription.
func InvokeSubscribeStart(lc fx.Lifecycle, sub *events.StartSubscriber, lg logging.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			if err := sub.Subscribe(context.Background()); err != nil {
				lg.Error("subscribe order saga start failed", logging.Err(err))
				return err
			}
			return nil
		},
	})
}

func ProvideGRPCServer(svc app.Service, cfg *config.Config, lg logging.Logger) (grpcsrv.Server, error) {
	return grpcsrv.NewServer(svc, cfg.GRPC, lg)
}

func InvokeRunGRPCServer(lc fx.Lifecycle, srv grpcsrv.Server) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { go func() { _ = srv.Start() }(); return nil },
		OnStop:  func(ctx context.Context) error { return srv.Shutdown(ctx) },
	})
}
