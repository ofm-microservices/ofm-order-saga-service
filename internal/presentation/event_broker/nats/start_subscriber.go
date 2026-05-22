package nats

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
)

const orderSagaStartDurable = "order_saga_start_durable"

// StartSubscriber wires the order start subject into the application service.
type StartSubscriber struct {
	broker app.EventBroker
	svc    app.Service
	cfg    app.Config
	log    logging.Logger
}

// NewStartSubscriber constructs the runtime subscriber.
func NewStartSubscriber(broker app.EventBroker, svc app.Service, cfg app.Config, log logging.Logger) *StartSubscriber {
	return &StartSubscriber{broker: broker, svc: svc, cfg: cfg, log: log}
}

// Subscribe registers the order start consumer.
func (s *StartSubscriber) Subscribe(ctx context.Context) error {
	return s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Stream:     s.cfg.OrderCommandsStream,
		Subject:    s.cfg.OrderSagaStartSubject,
		Durable:    orderSagaStartDurable,
		BatchSize:  s.cfg.CommandBatchSize,
		MaxWait:    s.cfg.CommandMaxWait,
		Workers:    1,
		QueueSize:  32,
		AckWait:    30 * time.Second,
		MaxDeliver: 5,
	}, s.handleOrderStart)
}

func (s *StartSubscriber) handleOrderStart(ctx context.Context, _ string, payload []byte) error {
	var msg app.OrderSagaCommand
	if err := json.Unmarshal(payload, &msg); err != nil {
		return err
	}
	return s.svc.Start(ctx, msg)
}
