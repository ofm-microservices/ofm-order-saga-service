package application

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	"order-saga-service/internal/domain"
)

type testBroker struct {
	mu        sync.Mutex
	published []publishedMessage
}

type publishedMessage struct {
	subject string
	payload []byte
}

func (b *testBroker) Publish(_ context.Context, subject string, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, publishedMessage{subject: subject, payload: append([]byte(nil), payload...)})
	return nil
}

func (b *testBroker) Subscribe(context.Context, string, MessageHandler) error { return nil }
func (b *testBroker) RunPullConsumer(context.Context, config.PullConsumerConfig, MessageHandler) error {
	return nil
}
func (b *testBroker) Close() {}

type testSessions struct {
	session *domain.Session
}

func (r *testSessions) Create(context.Context, domain.Session) (*domain.Session, error) {
	return nil, nil
}
func (r *testSessions) GetByID(context.Context, string) (*domain.Session, error) {
	return r.session, nil
}
func (r *testSessions) GetByOrderID(context.Context, string) (*domain.Session, error) {
	return nil, domain.ErrSessionNotFound
}
func (r *testSessions) UpdateStatus(context.Context, string, string) error { return nil }

type testSteps struct{}

func (r *testSteps) Create(context.Context, domain.Step) (*domain.Step, error) { return nil, nil }
func (r *testSteps) GetByKey(context.Context, string, string) (*domain.Step, error) {
	return &domain.Step{Status: domain.StepStatusPending}, nil
}
func (r *testSteps) ListBySagaID(context.Context, string) ([]domain.Step, error) { return nil, nil }
func (r *testSteps) UpdateStatus(context.Context, string, string, string) error  { return nil }

type testOrders struct{}

func (r *testOrders) CreateDraftOrder(context.Context, CreateDraftOrderCommand) (*CreateDraftOrderResult, error) {
	return nil, nil
}
func (r *testOrders) GetOrderPaymentSnapshot(context.Context, string) (*OrderPaymentSnapshot, error) {
	return nil, nil
}
func (r *testOrders) SaveRequirementAnswers(context.Context, SaveRequirementAnswersCommand) (*SaveRequirementAnswersResult, error) {
	return nil, nil
}
func (r *testOrders) SaveBuyerInitialMessage(context.Context, SaveBuyerInitialMessageCommand) (*SaveBuyerInitialMessageResult, error) {
	return nil, nil
}
func (r *testOrders) AttachFile(context.Context, AttachFileCommand) (*AttachFileResult, error) {
	return nil, nil
}
func (r *testOrders) MarkPaymentPending(context.Context, MarkPaymentPendingCommand) (*MarkPaymentPendingResult, error) {
	return nil, nil
}
func (r *testOrders) MarkOrderFunded(context.Context, MarkOrderFundedCommand) (*MarkOrderFundedResult, error) {
	return nil, nil
}
func (r *testOrders) MarkPaymentFailed(context.Context, MarkPaymentFailedCommand) (*MarkPaymentFailedResult, error) {
	return nil, nil
}
func (r *testOrders) Close() error { return nil }

type testPayments struct{}

func (r *testPayments) GetConnectStatus(context.Context, string) (*GetConnectStatusResult, error) {
	return nil, nil
}
func (r *testPayments) CreateCheckoutSession(context.Context, CreateCheckoutSessionCommand) (*CreateCheckoutSessionResult, error) {
	return nil, nil
}
func (r *testPayments) Close() error { return nil }

type testLogger struct{ logging.Logger }

func TestHandlePaymentStatusPublishesReceiptEmailAndRealtimeDelivery(t *testing.T) {
	logger, err := logging.New("order-saga-service", "test", "debug")
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}

	broker := &testBroker{}
	svcIface, err := New(
		&testSessions{session: &domain.Session{
			SagaID:               "saga-1",
			OrderID:              "order-1",
			BuyerID:              "user-1",
			BuyerEmail:           "buyer@example.com",
			RealtimeConnectionID: "startup-123.01JTEST",
			GigTitle:             "Logo design",
			PackageTier:          "Pro",
			PackageDescription:   "Fast delivery",
			PackageDeliveryDays:  3,
			PriceCents:           2599,
			Currency:             "USD",
			Status:               domain.SessionStatusPendingPayment,
		}},
		&testSteps{},
		&testGigClient{},
		&testAuthClient{},
		&testOrders{},
		&testPayments{},
		broker,
		config.NATSConfig{
			MailSendSubject:               "mail.send",
			RealtimeOrderConfirmedSubject: "realtime.order.confirmed",
		},
		logger,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	svc := svcIface
	err = svc.HandlePaymentStatus(context.Background(), PaymentStatusEvent{
		SagaID:          "saga-1",
		OrderID:         "order-1",
		PaymentIntentID: "pi_123",
		Status:          "payment.order_payment_succeeded",
		OccurredAt:      "2026-05-21T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("HandlePaymentStatus: %v", err)
	}
	if len(broker.published) != 3 {
		t.Fatalf("published messages = %d, want 3", len(broker.published))
	}
	for i, msg := range broker.published {
		t.Logf("publish[%d]=%q", i, msg.subject)
	}
	if broker.published[0].subject != "realtime.instance.startup-123" {
		t.Fatalf("first subject = %q, want %q", broker.published[0].subject, "realtime.instance.startup-123")
	}
	if broker.published[2].subject != "mail.send" {
		t.Fatalf("third subject = %q, want %q", broker.published[2].subject, "mail.send")
	}

	var realtimeMsg realtimeDeliveryMessage
	if err := json.Unmarshal(broker.published[0].payload, &realtimeMsg); err != nil {
		t.Fatalf("unmarshal realtime envelope: %v", err)
	}
	if realtimeMsg.ConnectionID != "startup-123.01JTEST" {
		t.Fatalf("realtime connection_id = %q, want %q", realtimeMsg.ConnectionID, "startup-123.01JTEST")
	}
	if realtimeMsg.UserID != "user-1" {
		t.Fatalf("realtime user_id = %q, want %q", realtimeMsg.UserID, "user-1")
	}
	if realtimeMsg.Type != "order.realtime" {
		t.Fatalf("realtime type = %q, want %q", realtimeMsg.Type, "order.realtime")
	}
	if len(realtimeMsg.Payload) == 0 {
		t.Fatal("realtime payload is empty")
	}

	var mailMsg MailSendCommand
	if err := json.Unmarshal(broker.published[2].payload, &mailMsg); err != nil {
		t.Fatalf("unmarshal mail command: %v", err)
	}
	if mailMsg.MessageType != "order_receipt" {
		t.Fatalf("mail message_type = %q, want %q", mailMsg.MessageType, "order_receipt")
	}
	if mailMsg.To != "buyer@example.com" {
		t.Fatalf("mail to = %q, want %q", mailMsg.To, "buyer@example.com")
	}
}

type testGigClient struct{}

func (c *testGigClient) GetOrderStartSnapshot(context.Context, string, string) (*OrderStartSnapshot, error) {
	return nil, nil
}

type testAuthClient struct{}

func (c *testAuthClient) GetEmailByUserID(context.Context, string) (string, error) { return "", nil }
func (c *testAuthClient) Close() error                                             { return nil }
