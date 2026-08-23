package appfx

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/idempotency"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
	eb "order-saga-service/internal/presentation/event_broker"
	broker "order-saga-service/internal/presentation/event_broker/kafka"
	scyllastore "order-saga-service/pkg/storage/scylla"

	"go.uber.org/fx"
)

// MessagingModule wires the Kafka broker adapter used by the order saga.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBrokerWithStore),
)

// ProvideEventBroker constructs the broker runtime.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.EventBroker, error) {
	return provideEventBroker(lc, cfg, lg, nil)
}

// ProvideEventBrokerWithStore wires Kafka with durable Scylla event claims.
func ProvideEventBrokerWithStore(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger, db *gocql.Session) (app.EventBroker, error) {
	return provideEventBroker(lc, cfg, lg, scyllastore.NewEventStore(db))
}

func provideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger, store idempotency.Store) (app.EventBroker, error) {
	eventBroker, err := broker.NewBroker(cfg.Kafka)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		eventBroker.Close()
		return nil
	}})
	return &eventBrokerAdapter{broker: eventBroker, store: store}, nil
}

type eventBrokerAdapter struct {
	broker eb.EventBroker
	store  idempotency.Store
}

func (a *eventBrokerAdapter) wrap(handler app.MessageHandler) eb.MessageHandler {
	return func(ctx context.Context, subject string, payload []byte) error {
		if a.store == nil {
			return handler(ctx, subject, payload)
		}
		event := idempotency.DecodeOrFingerprint(subject, payload)
		claimed, err := a.store.Claim(ctx, event)
		if err != nil {
			return err
		}
		if !claimed {
			return nil
		}
		if err := handler(ctx, subject, payload); err != nil {
			_ = a.store.Release(ctx, event.EventID)
			return err
		}
		return nil
	}
}

func (a *eventBrokerAdapter) Publish(ctx context.Context, subject string, payload []byte) error {
	return a.broker.Publish(ctx, subject, payload)
}

func (a *eventBrokerAdapter) Subscribe(ctx context.Context, subject string, handler app.MessageHandler) error {
	return a.broker.Subscribe(ctx, subject, a.wrap(handler))
}

func (a *eventBrokerAdapter) RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler app.MessageHandler) error {
	return a.broker.RunPullConsumer(ctx, cfg, a.wrap(handler))
}

func (a *eventBrokerAdapter) Close() {
	a.broker.Close()
}
