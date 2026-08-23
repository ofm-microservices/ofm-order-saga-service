package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	orderflowv1 "github.com/ofm-microservices/ofm-common/proto/orderflow/v1"
	paymentflowv1 "github.com/ofm-microservices/ofm-common/proto/paymentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
)

// StartSubscriber consumes order saga start commands from Kafka.
type StartSubscriber struct {
	broker app.EventBroker
	svc    app.Service
	cfg    config.KafkaConfig
	log    logging.Logger
}

// NewStartSubscriber constructs the Kafka order-start subscriber.

func NewStartSubscriber(b app.EventBroker, s app.Service, c config.KafkaConfig, l logging.Logger) *StartSubscriber {
	if b == nil || s == nil || l == nil {
		return nil
	}
	return &StartSubscriber{broker: b, svc: s, cfg: c, log: l}
}
func (s *StartSubscriber) Subscribe(ctx context.Context) error {
	return s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: s.cfg.StartTopic, Workers: 1, QueueSize: 32, AckWait: 30 * time.Second, MaxDeliver: 5}, s.handleStart)
}
func (s *StartSubscriber) handleStart(ctx context.Context, _ string, p []byte) error {
	var m app.OrderSagaCommand
	if err := json.Unmarshal(p, &m); err != nil {
		return err
	}
	return s.svc.Start(ctx, m)
}

// ResultSubscriber consumes order and payment results from Kafka.
type ResultSubscriber struct {
	broker app.EventBroker
	svc    app.Service
	cfg    config.KafkaConfig
	log    logging.Logger
}

// NewResultSubscriber constructs the Kafka result subscriber.

func NewResultSubscriber(b app.EventBroker, s app.Service, c config.KafkaConfig, l logging.Logger) *ResultSubscriber {
	if b == nil || s == nil || l == nil {
		return nil
	}
	return &ResultSubscriber{broker: b, svc: s, cfg: c, log: l}
}
func (s *ResultSubscriber) Subscribe(ctx context.Context) error {
	topics := []struct {
		topic   string
		handler app.MessageHandler
	}{{s.cfg.OrderResultTopic, s.orderResult}, {s.cfg.PaymentIntentTopic, s.paymentIntent}, {s.cfg.PaymentSucceededTopic, s.paymentStatus}, {s.cfg.PaymentFailedTopic, s.paymentStatus}, {s.cfg.ReleaseTopic, s.release}, {s.cfg.DeadLetterTopic, s.deadLetter}}
	for _, t := range topics {
		go func(t struct {
			topic   string
			handler app.MessageHandler
		}) {
			if err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: t.topic, Workers: 1, QueueSize: 32, AckWait: 30 * time.Second, MaxDeliver: 5}, t.handler); err != nil {
				s.log.Error("order-saga Kafka consumer failed", logging.String("topic", t.topic), logging.Err(err))
			}
		}(t)
	}
	return nil
}
func (s *ResultSubscriber) orderResult(ctx context.Context, _ string, p []byte) error {
	var m orderflowv1.OrderSagaResult
	if err := protojson.Unmarshal(p, &m); err != nil {
		return err
	}
	return s.svc.HandleOrderCreateResult(ctx, app.OrderSagaResult{SagaID: m.GetSagaId(), OrderID: m.GetOrderId(), Status: m.GetStatus(), Error: m.GetError(), Operation: m.GetOperation(), OccurredAt: m.GetOccurredAt()})
}
func (s *ResultSubscriber) paymentIntent(ctx context.Context, _ string, p []byte) error {
	var m paymentflowv1.PaymentIntentResult
	if err := protojson.Unmarshal(p, &m); err != nil {
		return err
	}
	return s.svc.HandlePaymentIntentResult(ctx, app.PaymentIntentResult{SagaID: m.GetSagaId(), OrderID: m.GetOrderId(), PaymentIntentID: m.GetPaymentIntentId(), CheckoutURL: m.GetCheckoutUrl(), ProviderIntentID: m.GetProviderIntentId(), Status: m.GetStatus(), Error: m.GetError(), OccurredAt: m.GetOccurredAt()})
}
func (s *ResultSubscriber) paymentStatus(ctx context.Context, _ string, p []byte) error {
	var m paymentflowv1.PaymentStatusEvent
	if err := protojson.Unmarshal(p, &m); err != nil {
		return err
	}
	return s.svc.HandlePaymentStatus(ctx, app.PaymentStatusEvent{SagaID: m.GetSagaId(), OrderID: m.GetOrderId(), PaymentIntentID: m.GetPaymentIntentId(), Status: m.GetStatus(), Error: m.GetError(), OccurredAt: m.GetOccurredAt()})
}

type releaseRequest struct {
	OrderID string `json:"order_id"`
}

func (s *ResultSubscriber) release(ctx context.Context, _ string, p []byte) error {
	var m releaseRequest
	if err := json.Unmarshal(p, &m); err != nil {
		return err
	}
	return s.svc.HandleReleaseFunds(ctx, m.OrderID)
}
func (s *ResultSubscriber) deadLetter(ctx context.Context, _ string, p []byte) error {
	s.log.Error("order-saga dead letter received", logging.String("payload", string(p)))
	return nil
}
