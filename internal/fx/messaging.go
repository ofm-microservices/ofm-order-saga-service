package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
	eb "order-saga-service/internal/presentation/event_broker"
	broker "order-saga-service/internal/presentation/event_broker/nats"

	"go.uber.org/fx"
)

// MessagingModule wires the NATS broker adapter.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBroker),
)

// ProvideEventBroker constructs the broker runtime.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.EventBroker, error) {
	eventBroker, err := broker.NewBroker(cfg.NATS, lg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		eventBroker.Close()
		return nil
	}})
	return &eventBrokerAdapter{broker: eventBroker}, nil
}

type eventBrokerAdapter struct {
	broker eb.EventBroker
}

func (a *eventBrokerAdapter) Publish(ctx context.Context, subject string, payload []byte) error {
	return a.broker.Publish(ctx, subject, payload)
}

func (a *eventBrokerAdapter) Subscribe(ctx context.Context, subject string, handler app.MessageHandler) error {
	return a.broker.Subscribe(ctx, subject, func(ctx context.Context, subject string, payload []byte) error {
		return handler(ctx, subject, payload)
	})
}

func (a *eventBrokerAdapter) RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler app.MessageHandler) error {
	return a.broker.RunPullConsumer(ctx, cfg, func(ctx context.Context, subject string, payload []byte) error {
		return handler(ctx, subject, payload)
	})
}

func (a *eventBrokerAdapter) Close() {
	a.broker.Close()
}
