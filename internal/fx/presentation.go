package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
	events "order-saga-service/internal/presentation/event_broker/kafka"
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
	fx.Provide(ProvideMeter),
	fx.Invoke(InvokeRunMetricsServer),
)

// ProvideMeter constructs the service-owned Prometheus meter.
func ProvideMeter(cfg *config.Config) sharedmetrics.Meter {
	meter := sharedmetrics.New(cfg.App.Name, cfg.App.Env)
	sharedmetrics.SetGlobal(meter)
	return meter
}

// InvokeRunMetricsServer exposes the service-owned Prometheus registry.
func InvokeRunMetricsServer(lc fx.Lifecycle, cfg *config.Config, meter sharedmetrics.Meter, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{OnStart: func(context.Context) error {
		runCtx, runCancel := context.WithCancel(context.Background())
		cancel = runCancel
		go func() {
			_ = sharedmetrics.StartServer(runCtx, sharedmetrics.Config{Enabled: cfg.Metrics.Enabled, Host: cfg.Metrics.Host, Port: cfg.Metrics.Port, Path: cfg.Metrics.Path}, meter, lg)
		}()
		return nil
	}, OnStop: func(context.Context) error {
		if cancel != nil {
			cancel()
		}
		return nil
	}})
}

// ProvideResultSubscriber constructs the saga result subscriber.
func ProvideResultSubscriber(broker app.EventBroker, svc app.Service, cfg *config.Config, lg logging.Logger) *events.ResultSubscriber {
	return events.NewResultSubscriber(broker, svc, cfg.Kafka, lg)
}

// ProvideStartSubscriber constructs the order start subscriber.
func ProvideStartSubscriber(broker app.EventBroker, svc app.Service, cfg *config.Config, lg logging.Logger) *events.StartSubscriber {
	return events.NewStartSubscriber(broker, svc, cfg.Kafka, lg)
}

// InvokeSubscribeResults starts the Kafka subscriptions.
func InvokeSubscribeResults(lc fx.Lifecycle, sub *events.ResultSubscriber, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			go func() {
				if err := sub.Subscribe(runCtx); err != nil && runCtx.Err() == nil {
					lg.Error("subscribe order saga results failed", logging.Err(err))
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}

// InvokeSubscribeStart starts the order start subscription.
func InvokeSubscribeStart(lc fx.Lifecycle, sub *events.StartSubscriber, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			go func() {
				if err := sub.Subscribe(runCtx); err != nil && runCtx.Err() == nil {
					lg.Error("subscribe order saga start failed", logging.Err(err))
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
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
