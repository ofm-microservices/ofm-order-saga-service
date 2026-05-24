package nats

import (
	"context"
	"testing"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
)

type recordingBroker struct {
	consumers []config.PullConsumerConfig
	handlers  []app.MessageHandler
}

func (b *recordingBroker) Publish(context.Context, string, []byte) error { return nil }
func (b *recordingBroker) Subscribe(context.Context, string, app.MessageHandler) error {
	return nil
}
func (b *recordingBroker) RunPullConsumer(_ context.Context, cfg config.PullConsumerConfig, handler app.MessageHandler) error {
	b.consumers = append(b.consumers, cfg)
	b.handlers = append(b.handlers, handler)
	return nil
}
func (b *recordingBroker) Close() {}

type recordingService struct {
	releaseFundsCalls []string
}

func (s *recordingService) Start(context.Context, app.OrderSagaCommand) error { return nil }
func (s *recordingService) HandleOrderCreateResult(context.Context, app.OrderSagaResult) error {
	return nil
}
func (s *recordingService) HandlePaymentIntentResult(context.Context, app.PaymentIntentResult) error {
	return nil
}
func (s *recordingService) HandlePaymentStatus(context.Context, app.PaymentStatusEvent) error {
	return nil
}
func (s *recordingService) HandleReleaseFunds(_ context.Context, orderID string) error {
	s.releaseFundsCalls = append(s.releaseFundsCalls, orderID)
	return nil
}
func (s *recordingService) StartOrder(context.Context, app.StartOrderCommand) (*app.StartOrderResult, error) {
	return nil, nil
}
func (s *recordingService) ConfirmOrder(context.Context, app.ConfirmOrderCommand) (*app.ConfirmOrderResult, error) {
	return nil, nil
}
func (s *recordingService) SubmitRequirements(context.Context, app.SubmitRequirementsCommand) (*app.SubmitRequirementsResult, error) {
	return nil, nil
}
func (s *recordingService) SubmitMessage(context.Context, app.SubmitMessageCommand) (*app.SubmitMessageResult, error) {
	return nil, nil
}
func (s *recordingService) DeliverOrder(context.Context, app.DeliverOrderCommand) (*app.DeliverOrderResult, error) {
	return nil, nil
}
func (s *recordingService) AcceptDelivery(context.Context, app.AcceptDeliveryCommand) (*app.AcceptDeliveryResult, error) {
	return nil, nil
}
func (s *recordingService) RequestRevision(context.Context, app.RequestRevisionCommand) (*app.RequestRevisionResult, error) {
	return nil, nil
}
func (s *recordingService) OpenDispute(context.Context, app.OpenDisputeCommand) (*app.OpenDisputeResult, error) {
	return nil, nil
}

func TestResultSubscriberSubscribeRegistersReleaseRequestAndDeadLetterConsumers(t *testing.T) {
	broker := &recordingBroker{}
	svc := &recordingService{}
	logger, err := logging.New("order-saga-service", "test", "debug")
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}
	s := NewResultSubscriber(broker, svc, config.NATSConfig{
		OrderEventsStream:          "ORDER_EVENTS",
		PaymentEventsStream:        "PAYMENT_EVENTS",
		OrderCommandsStream:        "ORDER_COMMANDS",
		OrderReleaseRequestSubject: "order.release.request",
		CommandBatchSize:           8,
		CommandMaxWait:             0,
	}, logger)

	if err := s.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(broker.consumers) != 6 {
		t.Fatalf("consumers = %d, want 6", len(broker.consumers))
	}
	if broker.consumers[4].Subject != "order.release.request" {
		t.Fatalf("release request subject = %q, want %q", broker.consumers[4].Subject, "order.release.request")
	}
	if broker.consumers[5].Subject != "order.saga.dead_letter" {
		t.Fatalf("dead letter subject = %q, want %q", broker.consumers[5].Subject, "order.saga.dead_letter")
	}
}

func TestResultSubscriberHandleReleaseFundsRequest(t *testing.T) {
	broker := &recordingBroker{}
	svc := &recordingService{}
	logger, _ := logging.New("order-saga-service", "test", "debug")
	s := NewResultSubscriber(broker, svc, config.NATSConfig{}, logger)

	if err := s.handleReleaseFundsRequest(context.Background(), "order.release.request", []byte(`{"saga_id":"saga-1","order_id":"order-1","requested_at":"2026-05-24T00:00:00Z"}`)); err != nil {
		t.Fatalf("handleReleaseFundsRequest: %v", err)
	}
	if len(svc.releaseFundsCalls) != 1 || svc.releaseFundsCalls[0] != "order-1" {
		t.Fatalf("releaseFundsCalls = %#v, want [order-1]", svc.releaseFundsCalls)
	}
}

func TestResultSubscriberHandleDeadLetter(t *testing.T) {
	broker := &recordingBroker{}
	svc := &recordingService{}
	logger, _ := logging.New("order-saga-service", "test", "debug")
	s := NewResultSubscriber(broker, svc, config.NATSConfig{}, logger)

	if err := s.handleDeadLetter(context.Background(), "order.saga.dead_letter", []byte(`{"saga_id":"saga-1","step_key":"payment.release_funds"}`)); err != nil {
		t.Fatalf("handleDeadLetter: %v", err)
	}
}

func TestResultSubscriberHandleReleaseFundsRequestRejectsInvalidJSON(t *testing.T) {
	broker := &recordingBroker{}
	svc := &recordingService{}
	logger, _ := logging.New("order-saga-service", "test", "debug")
	s := NewResultSubscriber(broker, svc, config.NATSConfig{}, logger)

	if err := s.handleReleaseFundsRequest(context.Background(), "order.release.request", []byte(`{`)); err == nil {
		t.Fatal("expected error")
	}
}

var _ app.Service = (*recordingService)(nil)
