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
func (r *testSessions) UpdateStatus(_ context.Context, _ string, status string) error {
	if r.session != nil {
		r.session.Status = status
	}
	return nil
}

type testSteps struct {
	statusByKey map[string]string
}

func (r *testSteps) Create(context.Context, domain.Step) (*domain.Step, error) { return nil, nil }
func (r *testSteps) GetByKey(_ context.Context, _ string, stepKey string) (*domain.Step, error) {
	if r.statusByKey == nil {
		return &domain.Step{Status: domain.StepStatusPending}, nil
	}
	status, ok := r.statusByKey[stepKey]
	if !ok {
		status = domain.StepStatusPending
	}
	return &domain.Step{Status: status}, nil
}
func (r *testSteps) ListBySagaID(context.Context, string) ([]domain.Step, error) { return nil, nil }
func (r *testSteps) UpdateStatus(context.Context, string, string, string) error  { return nil }

type testOrders struct {
	lifecycleSnapshot       *OrderLifecycleSnapshot
	markCompletedCalls      []MarkOrderCompletedCommand
	markPaymentFailedCalls  []MarkPaymentFailedCommand
	markReleaseFailedCalls  []MarkReleaseFailedCommand
	markReleasePendingCalls []MarkReleasePendingCommand
	saveDeliveryCalls       []SaveDeliveryCommand
	openDisputeCalls        []OpenDisputeCommand
	markOrderCompletedErr   error
	markPaymentFailedErr    error
	markReleasePendingErr   error
}

func (r *testOrders) CreateDraftOrder(context.Context, CreateDraftOrderCommand) (*CreateDraftOrderResult, error) {
	return nil, nil
}
func (r *testOrders) GetOrderPaymentSnapshot(context.Context, string) (*OrderPaymentSnapshot, error) {
	return nil, nil
}
func (r *testOrders) GetOrderLifecycleSnapshot(_ context.Context, _ string) (*OrderLifecycleSnapshot, error) {
	if r.lifecycleSnapshot != nil {
		return r.lifecycleSnapshot, nil
	}
	return &OrderLifecycleSnapshot{
		OrderID:            "order-1",
		SagaID:             "saga-1",
		BuyerID:            "user-1",
		SellerID:           "seller-1",
		GigID:              "gig-1",
		GigTitle:           "Logo design",
		PackageTitle:       "Pro",
		PackageDescription: "Fast delivery",
		PriceCents:         2599,
		Currency:           "USD",
		Status:             domain.SessionStatusFunded,
	}, nil
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
func (r *testOrders) MarkPaymentFailed(_ context.Context, cmd MarkPaymentFailedCommand) (*MarkPaymentFailedResult, error) {
	r.markPaymentFailedCalls = append(r.markPaymentFailedCalls, cmd)
	if r.markPaymentFailedErr != nil {
		return nil, r.markPaymentFailedErr
	}
	return &MarkPaymentFailedResult{OrderID: cmd.OrderID, Status: domain.SessionStatusFailed}, nil
}
func (r *testOrders) SaveDelivery(_ context.Context, cmd SaveDeliveryCommand) (*SaveDeliveryResult, error) {
	r.saveDeliveryCalls = append(r.saveDeliveryCalls, cmd)
	return nil, nil
}
func (r *testOrders) MarkReleasePending(_ context.Context, cmd MarkReleasePendingCommand) (*MarkReleasePendingResult, error) {
	r.markReleasePendingCalls = append(r.markReleasePendingCalls, cmd)
	if r.markReleasePendingErr != nil {
		return nil, r.markReleasePendingErr
	}
	return &MarkReleasePendingResult{OrderID: cmd.OrderID, Status: domain.SessionStatusReleasePending}, nil
}
func (r *testOrders) RequestRevision(context.Context, RequestRevisionCommand) (*RequestRevisionResult, error) {
	return nil, nil
}
func (r *testOrders) OpenDispute(_ context.Context, cmd OpenDisputeCommand) (*OpenDisputeResult, error) {
	r.openDisputeCalls = append(r.openDisputeCalls, cmd)
	return nil, nil
}
func (r *testOrders) MarkOrderCompleted(_ context.Context, cmd MarkOrderCompletedCommand) (*MarkOrderCompletedResult, error) {
	r.markCompletedCalls = append(r.markCompletedCalls, cmd)
	if r.markOrderCompletedErr != nil {
		return nil, r.markOrderCompletedErr
	}
	return &MarkOrderCompletedResult{OrderID: cmd.OrderID, Status: domain.SessionStatusCompleted}, nil
}
func (r *testOrders) MarkDisputeResolved(_ context.Context, cmd MarkDisputeResolvedCommand) (*MarkDisputeResolvedResult, error) {
	return &MarkDisputeResolvedResult{OrderID: cmd.OrderID, Status: domain.SessionStatusDisputeResolved}, nil
}
func (r *testOrders) MarkReleaseFailed(_ context.Context, cmd MarkReleaseFailedCommand) (*MarkReleaseFailedResult, error) {
	r.markReleaseFailedCalls = append(r.markReleaseFailedCalls, cmd)
	return &MarkReleaseFailedResult{OrderID: cmd.OrderID, Status: domain.SessionStatusReleaseFailed}, nil
}
func (r *testOrders) Close() error { return nil }

type testPayments struct {
	releaseCalls []ReleaseFundsCommand
	recovery     *GetReleaseByOrderResult
	releaseErr   error
	recoveryErr  error
}

func (r *testPayments) GetConnectStatus(context.Context, string) (*GetConnectStatusResult, error) {
	return nil, nil
}
func (r *testPayments) CreateCheckoutSession(context.Context, CreateCheckoutSessionCommand) (*CreateCheckoutSessionResult, error) {
	return nil, nil
}
func (r *testPayments) ReleaseFunds(_ context.Context, cmd ReleaseFundsCommand) (*ReleaseFundsResult, error) {
	r.releaseCalls = append(r.releaseCalls, cmd)
	if r.releaseErr != nil {
		return nil, r.releaseErr
	}
	return &ReleaseFundsResult{OrderID: cmd.OrderID, PaymentReleaseID: "release-1", StripeTransferID: "tr_1", Status: "released", OccurredAt: "2026-05-21T00:00:00Z"}, nil
}
func (r *testPayments) SettleDispute(_ context.Context, cmd SettleDisputeCommand) (*SettleDisputeResult, error) {
	return &SettleDisputeResult{
		OrderID:               cmd.OrderID,
		PaymentReleaseID:      "release-1",
		StripeTransferID:      "tr_1",
		StripeRefundID:        "re_1",
		FreelancerAmountCents: 1799,
		CustomerAmountCents:   800,
		Status:                "released",
		OccurredAt:            "2026-05-21T00:00:00Z",
	}, nil
}
func (r *testPayments) GetReleaseByOrderID(context.Context, string) (*GetReleaseByOrderResult, error) {
	if r.recoveryErr != nil {
		return nil, r.recoveryErr
	}
	return r.recovery, nil
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
			SagaID:              "saga-1",
			OrderID:             "order-1",
			BuyerID:             "user-1",
			BuyerEmail:          "buyer@example.com",
			GigTitle:            "Logo design",
			PackageTier:         "Pro",
			PackageDescription:  "Fast delivery",
			PackageDeliveryDays: 3,
			PriceCents:          2599,
			Currency:            "USD",
			Status:              domain.SessionStatusPendingPayment,
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
			OrderFundedSubject:            "order.funded",
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
	if broker.published[0].subject != "realtime" {
		t.Fatalf("first subject = %q, want %q", broker.published[0].subject, "realtime")
	}
	if broker.published[2].subject != "mail.send" {
		t.Fatalf("third subject = %q, want %q", broker.published[2].subject, "mail.send")
	}
	if broker.published[1].subject != "order.funded" {
		t.Fatalf("second subject = %q, want %q", broker.published[1].subject, "order.funded")
	}

	var realtimeMsg realtimeDeliveryMessage
	if err := json.Unmarshal(broker.published[0].payload, &realtimeMsg); err != nil {
		t.Fatalf("unmarshal realtime envelope: %v", err)
	}
	if realtimeMsg.UserID != "user-1" {
		t.Fatalf("realtime user_id = %q, want %q", realtimeMsg.UserID, "user-1")
	}
	if realtimeMsg.DeliveryScope != "user" {
		t.Fatalf("realtime delivery_scope = %q, want %q", realtimeMsg.DeliveryScope, "user")
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

func TestOpenDisputePublishesRealtimeNotificationsToBothParticipants(t *testing.T) {
	logger, err := logging.New("order-saga-service", "test", "debug")
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}

	broker := &testBroker{}
	orders := &testOrders{lifecycleSnapshot: &OrderLifecycleSnapshot{
		OrderID:               "order-1",
		SagaID:                "saga-1",
		BuyerID:               "buyer-1",
		SellerID:              "seller-1",
		GigID:                 "gig-1",
		GigTitle:              "Logo design",
		PackageID:             "package-1",
		PackageTitle:          "Pro",
		PackageDescription:    "Fast delivery",
		PriceCents:            2599,
		Currency:              "USD",
		Status:                domain.SessionStatusDelivered,
		RevisionCountSnapshot: 3,
		RevisionCountUsed:     3,
	}}
	svcIface, err := New(
		&testSessions{session: &domain.Session{
			SagaID:              "saga-1",
			OrderID:             "order-1",
			BuyerID:             "buyer-1",
			SellerID:            "seller-1",
			BuyerEmail:          "buyer@example.com",
			GigTitle:            "Logo design",
			PackageTier:         "Pro",
			PackageDescription:  "Fast delivery",
			PackageDeliveryDays: 3,
			PriceCents:          2599,
			Currency:            "USD",
			Status:              domain.SessionStatusDisputed,
		}},
		&testSteps{},
		&testGigClient{},
		&testAuthClient{},
		orders,
		&testPayments{},
		broker,
		config.NATSConfig{
			MailSendSubject:               "mail.send",
			RealtimeOrderConfirmedSubject: "realtime.order.confirmed",
			OrderDisputedSubject:          "order.disputed",
		},
		logger,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = svcIface.OpenDispute(context.Background(), OpenDisputeCommand{
		OrderID:     "order-1",
		ActorID:     "buyer-1",
		Reason:      "broken delivery",
		RequestedAt: "2026-05-21T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("OpenDispute: %v", err)
	}
	if len(broker.published) != 5 {
		t.Fatalf("published messages = %d, want 5", len(broker.published))
	}
	if broker.published[0].subject != "realtime" || broker.published[1].subject != "realtime" {
		t.Fatalf("first realtime subjects = %q, %q", broker.published[0].subject, broker.published[1].subject)
	}
	if broker.published[2].subject != "order.disputed" {
		t.Fatalf("third subject = %q, want %q", broker.published[2].subject, "order.disputed")
	}
	if broker.published[3].subject != "mail.send" || broker.published[4].subject != "mail.send" {
		t.Fatalf("mail subjects = %q, %q", broker.published[3].subject, broker.published[4].subject)
	}
	if len(orders.openDisputeCalls) != 1 {
		t.Fatalf("open dispute calls = %d, want 1", len(orders.openDisputeCalls))
	}
	if orders.openDisputeCalls[0].DisputeType != "buyer_dispute_after_delivery" {
		t.Fatalf("dispute type = %q, want %q", orders.openDisputeCalls[0].DisputeType, "buyer_dispute_after_delivery")
	}
}

func TestOpenDisputeAllowsSellerCancelBeforeDelivery(t *testing.T) {
	logger, err := logging.New("order-saga-service", "test", "debug")
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}

	broker := &testBroker{}
	orders := &testOrders{lifecycleSnapshot: &OrderLifecycleSnapshot{
		OrderID:    "order-1",
		SagaID:     "saga-1",
		BuyerID:    "buyer-1",
		SellerID:   "seller-1",
		GigTitle:   "Logo design",
		PriceCents: 10000,
		Currency:   "USD",
		Status:     domain.SessionStatusFunded,
	}}
	svcIface, err := New(
		&testSessions{session: &domain.Session{
			SagaID:     "saga-1",
			OrderID:    "order-1",
			BuyerID:    "buyer-1",
			SellerID:   "seller-1",
			BuyerEmail: "buyer@example.com",
			Status:     domain.SessionStatusDisputed,
		}},
		&testSteps{},
		&testGigClient{},
		&testAuthClient{},
		orders,
		&testPayments{},
		broker,
		config.NATSConfig{
			MailSendSubject:      "mail.send",
			OrderDisputedSubject: "order.disputed",
		},
		logger,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	res, err := svcIface.OpenDispute(context.Background(), OpenDisputeCommand{
		OrderID:     "order-1",
		ActorID:     "seller-1",
		Reason:      "seller cannot complete",
		RequestedAt: "2026-05-21T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("OpenDispute: %v", err)
	}
	if res.Status != domain.SessionStatusDisputed {
		t.Fatalf("status = %q, want %q", res.Status, domain.SessionStatusDisputed)
	}
	if len(orders.openDisputeCalls) != 1 {
		t.Fatalf("open dispute calls = %d, want 1", len(orders.openDisputeCalls))
	}
	if orders.openDisputeCalls[0].DisputeType != "seller_cancel_before_delivery" {
		t.Fatalf("dispute type = %q, want %q", orders.openDisputeCalls[0].DisputeType, "seller_cancel_before_delivery")
	}
}

func TestOpenDisputeClassifiesBuyerCancelBeforeDelivery(t *testing.T) {
	logger, err := logging.New("order-saga-service", "test", "debug")
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}

	orders := &testOrders{lifecycleSnapshot: &OrderLifecycleSnapshot{
		OrderID:    "order-1",
		SagaID:     "saga-1",
		BuyerID:    "buyer-1",
		SellerID:   "seller-1",
		GigTitle:   "Logo design",
		PriceCents: 10000,
		Currency:   "USD",
		Status:     domain.SessionStatusFunded,
	}}
	svcIface, err := New(
		&testSessions{session: &domain.Session{SagaID: "saga-1", OrderID: "order-1", BuyerID: "buyer-1", SellerID: "seller-1", Status: domain.SessionStatusDisputed}},
		&testSteps{},
		&testGigClient{},
		&testAuthClient{},
		orders,
		&testPayments{},
		&testBroker{},
		config.NATSConfig{MailSendSubject: "mail.send", OrderDisputedSubject: "order.disputed"},
		logger,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = svcIface.OpenDispute(context.Background(), OpenDisputeCommand{
		OrderID:     "order-1",
		ActorID:     "buyer-1",
		Reason:      "buyer changed plans",
		RequestedAt: "2026-05-21T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("OpenDispute: %v", err)
	}
	if len(orders.openDisputeCalls) != 1 {
		t.Fatalf("open dispute calls = %d, want 1", len(orders.openDisputeCalls))
	}
	if orders.openDisputeCalls[0].DisputeType != "buyer_cancel_before_delivery" {
		t.Fatalf("dispute type = %q, want %q", orders.openDisputeCalls[0].DisputeType, "buyer_cancel_before_delivery")
	}
}

func TestResolveDisputePublishesRealtimeNotificationsToBothParticipants(t *testing.T) {
	logger, err := logging.New("order-saga-service", "test", "debug")
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}

	broker := &testBroker{}
	svcIface, err := New(
		&testSessions{session: &domain.Session{
			SagaID:              "saga-1",
			OrderID:             "order-1",
			BuyerID:             "buyer-1",
			SellerID:            "seller-1",
			BuyerEmail:          "buyer@example.com",
			GigTitle:            "Logo design",
			PackageTier:         "Pro",
			PackageDescription:  "Fast delivery",
			PackageDeliveryDays: 3,
			PriceCents:          2599,
			Currency:            "USD",
			Status:              domain.SessionStatusDisputeResolved,
		}},
		&testSteps{},
		&testGigClient{},
		&testAuthClient{},
		&testOrders{lifecycleSnapshot: &OrderLifecycleSnapshot{
			OrderID:               "order-1",
			SagaID:                "saga-1",
			BuyerID:               "buyer-1",
			SellerID:              "seller-1",
			GigID:                 "gig-1",
			GigTitle:              "Logo design",
			PackageID:             "package-1",
			PackageTitle:          "Pro",
			PackageDescription:    "Fast delivery",
			PriceCents:            2599,
			Currency:              "USD",
			PaymentIntentID:       "pi_123",
			PaymentReleaseID:      "release-1",
			RevisionCountSnapshot: 3,
			RevisionCountUsed:     3,
			Status:                domain.SessionStatusDisputed,
		}},
		&testPayments{},
		broker,
		config.NATSConfig{
			MailSendSubject:               "mail.send",
			RealtimeOrderConfirmedSubject: "realtime.order.confirmed",
			OrderDisputeResolvedSubject:   "order.dispute_resolved",
		},
		logger,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = svcIface.ResolveDispute(context.Background(), SettleDisputeCommand{
		OrderID:              "order-1",
		AdminUserID:          "admin-1",
		FreelancerPercentage: 70,
		CustomerPercentage:   30,
		Reason:               "split the funds",
		RequestedAt:          "2026-05-21T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("ResolveDispute: %v", err)
	}
	if len(broker.published) != 5 {
		t.Fatalf("published messages = %d, want 5", len(broker.published))
	}
	if broker.published[0].subject != "realtime" || broker.published[1].subject != "realtime" {
		t.Fatalf("first realtime subjects = %q, %q", broker.published[0].subject, broker.published[1].subject)
	}
	if broker.published[2].subject != "order.dispute_resolved" {
		t.Fatalf("third subject = %q, want %q", broker.published[2].subject, "order.dispute_resolved")
	}
	if broker.published[3].subject != "mail.send" || broker.published[4].subject != "mail.send" {
		t.Fatalf("mail subjects = %q, %q", broker.published[3].subject, broker.published[4].subject)
	}
}

type testGigClient struct{}

func (c *testGigClient) GetOrderStartSnapshot(context.Context, string, string) (*OrderStartSnapshot, error) {
	return &OrderStartSnapshot{
		GigID:              "gig-1",
		PackageID:          "package-1",
		SellerID:           "seller-1",
		SellerUsername:     "seller",
		GigTitle:           "Logo design",
		PackageTitle:       "Pro",
		PackageDescription: "Fast delivery",
		PriceCents:         2599,
		Currency:           "USD",
		DeliveryDays:       3,
		RevisionCount:      2,
		GigPublished:       true,
		PackageAvailable:   true,
		Questions: []OrderStartQuestion{
			{ID: "q-1", Text: "What do you need built?", SortOrder: 1},
		},
	}, nil
}

type testAuthClient struct {
	called bool
}

func (c *testAuthClient) GetEmailByUserID(context.Context, string) (string, error) {
	c.called = true
	return "", nil
}
func (c *testAuthClient) Close() error { return nil }

func TestStartOrderUsesBuyerEmailFromCommandAndDoesNotRequireAuthLookup(t *testing.T) {
	logger, err := logging.New("order-saga-service", "test", "debug")
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}

	auth := &testAuthClient{}
	svc, err := New(
		&testSessions{},
		&testSteps{},
		&testGigClient{},
		auth,
		&testOrders{lifecycleSnapshot: &OrderLifecycleSnapshot{
			OrderID:      "order-1",
			SagaID:       "saga-1",
			BuyerID:      "buyer-1",
			SellerID:     "seller-1",
			GigID:        "gig-1",
			GigTitle:     "Logo design",
			PackageID:    "package-1",
			PackageTitle: "Pro",
			Status:       domain.SessionStatusStarted,
		}},
		&testPayments{},
		&testBroker{},
		config.NATSConfig{},
		logger,
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	res, err := svc.StartOrder(context.Background(), StartOrderCommand{
		BuyerID:     "buyer-1",
		BuyerEmail:  "buyer@example.com",
		GigID:       "gig-1",
		PackageID:   "package-1",
		OrderID:     "order-1",
		SagaID:      "saga-1",
		RequestedAt: "2026-05-21T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("StartOrder: %v", err)
	}
	if res == nil || res.Snapshot == nil {
		t.Fatal("StartOrder returned nil snapshot")
	}
	if res.Snapshot.SellerID != "seller-1" {
		t.Fatalf("seller id = %q, want %q", res.Snapshot.SellerID, "seller-1")
	}
	if auth.called {
		t.Fatal("auth lookup was called even though buyer_email was provided")
	}
}
