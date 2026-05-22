package nats

import (
	"context"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	orderflowv1 "github.com/ofm-microservices/ofm-common/proto/orderflow/v1"
	paymentflowv1 "github.com/ofm-microservices/ofm-common/proto/paymentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
)

const (
	orderCreateResultDurable     = "order_create_result_durable"
	paymentIntentResultDurable   = "payment_intent_result_durable"
	paymentOrderSucceededDurable = "payment_order_succeeded_durable"
	paymentOrderFailedDurable    = "payment_order_failed_durable"
)

// ResultSubscriber wires the saga result subjects into the application service.
type ResultSubscriber struct {
	broker app.EventBroker
	svc    app.Service
	cfg    config.NATSConfig
	log    logging.Logger
}

// NewResultSubscriber constructs the runtime subscriber.
func NewResultSubscriber(broker app.EventBroker, svc app.Service, cfg config.NATSConfig, log logging.Logger) *ResultSubscriber {
	return &ResultSubscriber{broker: broker, svc: svc, cfg: cfg, log: log}
}

// Subscribe registers the saga consumers.
func (s *ResultSubscriber) Subscribe(ctx context.Context) error {
	if err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Stream:     s.cfg.OrderEventsStream,
		Subject:    s.cfg.OrderCreateResultSubject,
		Durable:    orderCreateResultDurable,
		BatchSize:  s.cfg.CommandBatchSize,
		MaxWait:    s.cfg.CommandMaxWait,
		Workers:    1,
		QueueSize:  32,
		AckWait:    30 * time.Second,
		MaxDeliver: 5,
	}, s.handleOrderCreateResult); err != nil {
		return err
	}
	if err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Stream:     s.cfg.PaymentEventsStream,
		Subject:    s.cfg.PaymentIntentResultSubject,
		Durable:    paymentIntentResultDurable,
		BatchSize:  s.cfg.CommandBatchSize,
		MaxWait:    s.cfg.CommandMaxWait,
		Workers:    1,
		QueueSize:  32,
		AckWait:    30 * time.Second,
		MaxDeliver: 5,
	}, s.handlePaymentIntentResult); err != nil {
		return err
	}
	if err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Stream:     s.cfg.PaymentEventsStream,
		Subject:    s.cfg.PaymentOrderSucceededSubject,
		Durable:    paymentOrderSucceededDurable,
		BatchSize:  s.cfg.CommandBatchSize,
		MaxWait:    s.cfg.CommandMaxWait,
		Workers:    1,
		QueueSize:  32,
		AckWait:    30 * time.Second,
		MaxDeliver: 5,
	}, s.handlePaymentStatus); err != nil {
		return err
	}
	if err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Stream:     s.cfg.PaymentEventsStream,
		Subject:    s.cfg.PaymentOrderFailedSubject,
		Durable:    paymentOrderFailedDurable,
		BatchSize:  s.cfg.CommandBatchSize,
		MaxWait:    s.cfg.CommandMaxWait,
		Workers:    1,
		QueueSize:  32,
		AckWait:    30 * time.Second,
		MaxDeliver: 5,
	}, s.handlePaymentStatus); err != nil {
		return err
	}
	return nil
}

func (s *ResultSubscriber) handleOrderCreateResult(ctx context.Context, _ string, payload []byte) error {
	var msg orderflowv1.OrderSagaResult
	if err := protojson.Unmarshal(payload, &msg); err != nil {
		return err
	}
	return s.svc.HandleOrderCreateResult(ctx, app.OrderSagaResult{
		SagaID:     msg.GetSagaId(),
		OrderID:    msg.GetOrderId(),
		Status:     msg.GetStatus(),
		Error:      msg.GetError(),
		Operation:  msg.GetOperation(),
		OccurredAt: msg.GetOccurredAt(),
	})
}

func (s *ResultSubscriber) handlePaymentIntentResult(ctx context.Context, _ string, payload []byte) error {
	var msg paymentflowv1.PaymentIntentResult
	if err := protojson.Unmarshal(payload, &msg); err != nil {
		return err
	}
	return s.svc.HandlePaymentIntentResult(ctx, app.PaymentIntentResult{
		SagaID:           msg.GetSagaId(),
		OrderID:          msg.GetOrderId(),
		PaymentIntentID:  msg.GetPaymentIntentId(),
		CheckoutURL:      msg.GetCheckoutUrl(),
		ProviderIntentID: msg.GetProviderIntentId(),
		Status:           msg.GetStatus(),
		Error:            msg.GetError(),
		OccurredAt:       msg.GetOccurredAt(),
	})
}

func (s *ResultSubscriber) handlePaymentStatus(ctx context.Context, _ string, payload []byte) error {
	var msg paymentflowv1.PaymentStatusEvent
	if err := protojson.Unmarshal(payload, &msg); err != nil {
		return err
	}
	return s.svc.HandlePaymentStatus(ctx, app.PaymentStatusEvent{
		SagaID:          msg.GetSagaId(),
		OrderID:         msg.GetOrderId(),
		PaymentIntentID: msg.GetPaymentIntentId(),
		Status:          msg.GetStatus(),
		Error:           msg.GetError(),
		OccurredAt:      msg.GetOccurredAt(),
	})
}
