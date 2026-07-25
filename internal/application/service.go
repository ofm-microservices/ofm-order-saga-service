package application

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	orderflowv1 "github.com/ofm-microservices/ofm-common/proto/orderflow/v1"
	paymentflowv1 "github.com/ofm-microservices/ofm-common/proto/paymentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"order-saga-service/config"
	"order-saga-service/internal/domain"
)

type service struct {
	broker   EventBroker
	sessions SessionRepository
	steps    StepRepository
	gigs     GigSnapshotClient
	auth     AuthQueryClient
	orders   OrderWriteClient
	payments PaymentCheckoutClient
	mapr     *messageMapper
	cfg      config.NATSConfig
	log      Logger
}

// New constructs the order saga orchestrator.
func New(sessions SessionRepository, steps StepRepository, gigs GigSnapshotClient, auth AuthQueryClient, orders OrderWriteClient, payments PaymentCheckoutClient, broker EventBroker, cfg config.NATSConfig, log Logger) (Service, error) {
	if sessions == nil {
		return nil, ErrNilSessionRepository
	}
	if steps == nil {
		return nil, ErrNilStepRepository
	}
	if gigs == nil {
		return nil, ErrNilGigSnapshotClient
	}
	if auth == nil {
		return nil, ErrNilAuthQueryClient
	}
	if orders == nil {
		return nil, ErrNilOrderWriteClient
	}
	if payments == nil {
		return nil, ErrNilPaymentCheckoutClient
	}
	if broker == nil {
		return nil, ErrNilEventBroker
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &service{
		sessions: sessions,
		steps:    steps,
		gigs:     gigs,
		auth:     auth,
		orders:   orders,
		payments: payments,
		broker:   broker,
		mapr:     newMessageMapper(),
		cfg:      cfg,
		log:      log.With(logging.String("module", "application")),
	}, nil
}

func (s *service) Start(ctx context.Context, cmd OrderSagaCommand) error {
	if cmd.RequestedAt == "" {
		cmd.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if strings.TrimSpace(cmd.SagaID) == "" {
		cmd.SagaID = uuid.Must(uuid.NewV7()).String()
	}
	if strings.TrimSpace(cmd.OrderID) == "" {
		cmd.OrderID = uuid.Must(uuid.NewV7()).String()
	}
	snapshot, err := s.gigs.GetOrderStartSnapshot(ctx, cmd.GigID, cmd.PackageID)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return ErrInvalidOrderSnapshot
	}
	if strings.TrimSpace(snapshot.SellerID) == strings.TrimSpace(cmd.BuyerID) {
		return ErrSelfOrderNotAllowed
	}
	buyerEmail, err := s.auth.GetEmailByUserID(ctx, cmd.BuyerID)
	if err != nil {
		return err
	}
	session := domain.Session{
		SagaID:              cmd.SagaID,
		OrderID:             cmd.OrderID,
		BuyerID:             cmd.BuyerID,
		SellerID:            snapshot.SellerID,
		SellerUsername:      snapshot.SellerUsername,
		BuyerEmail:          buyerEmail,
		GigID:               snapshot.GigID,
		GigTitle:            snapshot.GigTitle,
		PictureFileID:       snapshot.PictureFileID,
		PackageID:           snapshot.PackageID,
		PackageTier:         snapshot.PackageTitle,
		PackageDescription:  snapshot.PackageDescription,
		PackageDeliveryDays: snapshot.DeliveryDays,
		PriceCents:          snapshot.PriceCents,
		Currency:            snapshot.Currency,
		Status:              domain.SessionStatusRequirementsPending,
	}
	if _, err := s.sessions.Create(ctx, session); err != nil {
		return err
	}
	for _, step := range []domain.Step{
		{SagaID: cmd.SagaID, StepKey: domain.StepKeyCreateOrder, Status: domain.StepStatusPending},
		{SagaID: cmd.SagaID, StepKey: domain.StepKeyCreatePaymentIntent, Status: domain.StepStatusPending},
		{SagaID: cmd.SagaID, StepKey: domain.StepKeyAwaitPaymentWebhook, Status: domain.StepStatusPending},
		{SagaID: cmd.SagaID, StepKey: domain.StepKeyRealtimeOrderAccepted, Status: domain.StepStatusPending},
		{SagaID: cmd.SagaID, StepKey: domain.StepKeyRealtimePaymentReady, Status: domain.StepStatusPending},
		{SagaID: cmd.SagaID, StepKey: domain.StepKeyRealtimeOrderConfirmed, Status: domain.StepStatusPending},
		{SagaID: cmd.SagaID, StepKey: domain.StepKeyRealtimeOrderFailed, Status: domain.StepStatusPending},
		{SagaID: cmd.SagaID, StepKey: domain.StepKeySendReceipt, Status: domain.StepStatusPending},
	} {
		if _, err := s.steps.Create(ctx, step); err != nil {
			return err
		}
	}
	orderPayload, err := protojson.Marshal(&orderflowv1.OrderCreateCommand{
		SagaId:              cmd.SagaID,
		OrderId:             cmd.OrderID,
		BuyerId:             cmd.BuyerID,
		SellerId:            snapshot.SellerID,
		SellerUsername:      snapshot.SellerUsername,
		GigId:               snapshot.GigID,
		GigTitle:            snapshot.GigTitle,
		PictureFileId:       snapshot.PictureFileID,
		PackageId:           snapshot.PackageID,
		PackageTier:         snapshot.PackageTitle,
		PackageDescription:  snapshot.PackageDescription,
		PackageDeliveryDays: snapshot.DeliveryDays,
		PriceCents:          snapshot.PriceCents,
		Currency:            snapshot.Currency,
		IdempotencyKey:      cmd.IdempotencyKey,
		RequestedAt:         cmd.RequestedAt,
	})
	if err != nil {
		return ErrPublishCommand
	}
	if err := s.steps.UpdateStatus(ctx, cmd.SagaID, domain.StepKeyCreateOrder, domain.StepStatusInProgress); err != nil {
		return err
	}
	if err := s.sessions.UpdateStatus(ctx, cmd.SagaID, domain.SessionStatusPendingOrder); err != nil {
		return err
	}
	if err := s.broker.Publish(ctx, s.cfg.OrderCreateSubject, orderPayload); err != nil {
		return err
	}
	return s.publishOrderAcceptedNotification(ctx, cmd.SagaID)
}

func (s *service) StartOrder(ctx context.Context, cmd StartOrderCommand) (*StartOrderResult, error) {
	if cmd.RequestedAt == "" {
		cmd.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if strings.TrimSpace(cmd.SagaID) == "" {
		cmd.SagaID = uuid.Must(uuid.NewV7()).String()
	}
	if strings.TrimSpace(cmd.OrderID) == "" {
		cmd.OrderID = uuid.Must(uuid.NewV7()).String()
	}
	snapshot, err := s.gigs.GetOrderStartSnapshot(ctx, cmd.GigID, cmd.PackageID)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, ErrInvalidOrderSnapshot
	}
	if strings.TrimSpace(snapshot.SellerID) == strings.TrimSpace(cmd.BuyerID) {
		return nil, ErrSelfOrderNotAllowed
	}
	buyerEmail := strings.TrimSpace(cmd.BuyerEmail)
	if buyerEmail == "" {
		var err error
		buyerEmail, err = s.auth.GetEmailByUserID(ctx, cmd.BuyerID)
		if err != nil {
			return nil, err
		}
	}
	session := domain.Session{
		SagaID:              cmd.SagaID,
		OrderID:             cmd.OrderID,
		BuyerID:             cmd.BuyerID,
		SellerID:            snapshot.SellerID,
		SellerUsername:      snapshot.SellerUsername,
		BuyerEmail:          buyerEmail,
		GigID:               snapshot.GigID,
		GigTitle:            snapshot.GigTitle,
		PackageID:           snapshot.PackageID,
		PackageTier:         snapshot.PackageTitle,
		PackageDescription:  snapshot.PackageDescription,
		PackageDeliveryDays: snapshot.DeliveryDays,
		PriceCents:          snapshot.PriceCents,
		Currency:            snapshot.Currency,
		Status:              domain.SessionStatusStarted,
	}
	if _, err := s.sessions.Create(ctx, session); err != nil {
		return nil, err
	}
	_ = s.sessions.UpdateStatus(ctx, cmd.SagaID, domain.SessionStatusRequirementsPending)
	if _, err := s.orders.CreateDraftOrder(ctx, CreateDraftOrderCommand{
		SagaID:              cmd.SagaID,
		OrderID:             cmd.OrderID,
		BuyerID:             cmd.BuyerID,
		SellerID:            snapshot.SellerID,
		SellerUsername:      snapshot.SellerUsername,
		GigID:               snapshot.GigID,
		GigTitle:            snapshot.GigTitle,
		PictureFileID:       snapshot.PictureFileID,
		PackageID:           snapshot.PackageID,
		PackageTier:         snapshot.PackageTitle,
		PackageDescription:  snapshot.PackageDescription,
		PackageDeliveryDays: snapshot.DeliveryDays,
		PriceCents:          snapshot.PriceCents,
		Currency:            snapshot.Currency,
		Questions:           convertQuestions(snapshot.Questions),
		IdempotencyKey:      cmd.IdempotencyKey,
		RequestedAt:         cmd.RequestedAt,
	}); err != nil {
		return nil, err
	}
	if err := s.publishChatCreate(ctx, cmd.OrderID, cmd.BuyerID, snapshot.SellerID); err != nil {
		return nil, err
	}
	return &StartOrderResult{
		SagaID:   cmd.SagaID,
		OrderID:  cmd.OrderID,
		Status:   domain.SessionStatusRequirementsPending,
		Snapshot: snapshot,
	}, nil
}

func convertQuestions(questions []OrderStartQuestion) []OrderQuestionSnapshot {
	out := make([]OrderQuestionSnapshot, 0, len(questions))
	for _, q := range questions {
		out = append(out, OrderQuestionSnapshot{
			QuestionID:  q.ID,
			Text:        q.Text,
			Type:        "text",
			Required:    true,
			OptionsJSON: "[]",
			SortOrder:   q.SortOrder,
		})
	}
	return out
}

func (s *service) ConfirmOrder(ctx context.Context, cmd ConfirmOrderCommand) (*ConfirmOrderResult, error) {
	if cmd.RequestedAt == "" {
		cmd.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	snap, err := s.orders.GetOrderPaymentSnapshot(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if !snap.RequirementsCompleted {
		return nil, ErrOrderRequirementsIncomplete
	}
	if !snap.MessageCompleted {
		return nil, ErrOrderMessageIncomplete
	}
	if strings.TrimSpace(snap.BuyerID) != strings.TrimSpace(cmd.BuyerID) {
		return nil, ErrOrderNotOwned
	}
	switch snap.Status {
	case domain.SessionStatusRequirementsPending, domain.SessionStatusStarted:
		// confirmable
	case domain.SessionStatusPendingPayment:
		return nil, ErrOrderAlreadyPaymentPending
	case domain.SessionStatusPaymentConfirmed, domain.SessionStatusCompleted:
		return nil, ErrOrderAlreadyFunded
	default:
		return nil, ErrOrderNotConfirmable
	}
	connect, err := s.payments.GetConnectStatus(ctx, snap.SellerID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(connect.Status) != "completed" {
		return nil, ErrConnectOnboardingIncomplete
	}
	checkout, err := s.payments.CreateCheckoutSession(ctx, CreateCheckoutSessionCommand{
		SagaID:         snap.SagaID,
		OrderID:        snap.OrderID,
		BuyerID:        snap.BuyerID,
		SellerID:       snap.SellerID,
		AmountCents:    snap.PriceCents,
		Currency:       snap.Currency,
		Title:          snap.GigTitle,
		IdempotencyKey: cmd.IdempotencyKey,
		RequestedAt:    cmd.RequestedAt,
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.orders.MarkPaymentPending(ctx, MarkPaymentPendingCommand{
		OrderID:         snap.OrderID,
		PaymentIntentID: checkout.PaymentIntentID,
		CheckoutURL:     checkout.CheckoutURL,
		RequestedAt:     cmd.RequestedAt,
	}); err != nil {
		return nil, err
	}
	if err := s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusPendingPayment); err != nil {
		return nil, err
	}
	return &ConfirmOrderResult{
		SagaID:      snap.SagaID,
		OrderID:     snap.OrderID,
		Status:      "payment_pending",
		CheckoutURL: checkout.CheckoutURL,
		PaymentID:   checkout.PaymentIntentID,
	}, nil
}

func (s *service) SubmitRequirements(ctx context.Context, cmd SubmitRequirementsCommand) (*SubmitRequirementsResult, error) {
	if _, err := s.orders.SaveRequirementAnswers(ctx, SaveRequirementAnswersCommand{OrderID: cmd.OrderID, Answers: cmd.Answers}); err != nil {
		return nil, err
	}
	return &SubmitRequirementsResult{OrderID: cmd.OrderID, Status: "requirements_completed", CurrentStep: "message_pending"}, nil
}

func (s *service) SubmitMessage(ctx context.Context, cmd SubmitMessageCommand) (*SubmitMessageResult, error) {
	if _, err := s.orders.SaveBuyerInitialMessage(ctx, SaveBuyerInitialMessageCommand{OrderID: cmd.OrderID, Message: cmd.Message}); err != nil {
		return nil, err
	}
	return &SubmitMessageResult{OrderID: cmd.OrderID, Status: "message_completed", CurrentStep: "attachments_pending"}, nil
}

func (s *service) DeliverOrder(ctx context.Context, cmd DeliverOrderCommand) (*DeliverOrderResult, error) {
	if cmd.RequestedAt == "" {
		cmd.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	snap, err := s.orders.GetOrderLifecycleSnapshot(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(snap.SellerID) != strings.TrimSpace(cmd.SellerID) {
		return nil, ErrOrderNotOwned
	}
	if snap.Status != domain.SessionStatusFunded && snap.Status != domain.SessionStatusRevisionRequested {
		return nil, ErrOrderNotDeliverable
	}
	if _, err := s.orders.SaveDelivery(ctx, SaveDeliveryCommand{
		OrderID:       cmd.OrderID,
		SellerID:      cmd.SellerID,
		Message:       cmd.Message,
		AttachmentIDs: cmd.AttachmentIDs,
		RequestedAt:   cmd.RequestedAt,
	}); err != nil {
		return nil, err
	}
	_ = s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusDelivered)
	if err := s.publishOrderLifecycle(ctx, snap.SagaID, domain.StepKeyDeliverOrder, domain.SessionStatusDelivered, "order_delivered", "Your delivery has been submitted.", "success", snap, cmd.SellerID, false); err != nil {
		s.log.Warn("failed to publish order delivery lifecycle notification",
			logging.Operation("order.delivery.notification"),
			logging.String("order_id", snap.OrderID),
			logging.Err(err),
		)
	}
	if err := s.publishLifecycleMail(ctx, snap, domain.SessionStatusDelivered, cmd.SellerID, "order_delivery_submitted_seller", "order_delivered", "Your delivery has been submitted."); err != nil {
		s.log.Warn("failed to publish order delivery mail",
			logging.Operation("order.delivery.mail"),
			logging.String("order_id", snap.OrderID),
			logging.String("user_id", cmd.SellerID),
			logging.Err(err),
		)
	}
	return &DeliverOrderResult{OrderID: snap.OrderID, Status: domain.SessionStatusDelivered, CurrentStep: "buyer_review_pending"}, nil
}

func (s *service) AcceptDelivery(ctx context.Context, cmd AcceptDeliveryCommand) (*AcceptDeliveryResult, error) {
	if cmd.RequestedAt == "" {
		cmd.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	snap, err := s.orders.GetOrderLifecycleSnapshot(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(snap.BuyerID) != strings.TrimSpace(cmd.BuyerID) {
		return nil, ErrOrderNotOwned
	}
	if snap.Status != domain.SessionStatusDelivered {
		return nil, ErrOrderNotAcceptable
	}
	if _, err := s.orders.MarkReleasePending(ctx, MarkReleasePendingCommand{OrderID: snap.OrderID, PaymentReleaseID: snap.PaymentReleaseID, RequestedAt: cmd.RequestedAt}); err != nil {
		return nil, err
	}
	if err := s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusReleasePending); err != nil {
		return nil, err
	}
	if _, err := s.ensureNotificationStep(ctx, snap.SagaID, domain.StepKeyReleaseFunds); err != nil {
		return nil, err
	}
	payload, err := s.mapr.marshal(releaseFundsRequestMessage{
		SagaID:      snap.SagaID,
		OrderID:     snap.OrderID,
		RequestedAt: cmd.RequestedAt,
	})
	if err != nil {
		return nil, ErrPublishCommand
	}
	if err := s.broker.Publish(ctx, s.cfg.OrderReleaseRequestSubject, payload); err != nil {
		return nil, err
	}
	return &AcceptDeliveryResult{OrderID: snap.OrderID, Status: domain.SessionStatusReleasePending, CurrentStep: string(domain.StepKeyReleaseFunds)}, nil
}

func (s *service) HandleReleaseFunds(ctx context.Context, orderID string) error {
	snap, err := s.orders.GetOrderLifecycleSnapshot(ctx, orderID)
	if err != nil {
		return err
	}
	if snap.Status == domain.SessionStatusCompleted {
		return nil
	}
	if snap.Status != domain.SessionStatusReleasePending && snap.Status != domain.SessionStatusDelivered {
		return ErrOrderNotAcceptable
	}
	releaseState, err := s.payments.GetReleaseByOrderID(ctx, snap.OrderID)
	if err == nil && releaseState != nil && releaseState.Status == "released" {
		if _, err := s.orders.MarkOrderCompleted(ctx, MarkOrderCompletedCommand{OrderID: snap.OrderID, PaymentReleaseID: releaseState.PaymentReleaseID, RequestedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
			return err
		}
		_ = s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusCompleted)
		_ = s.publishOrderLifecycle(ctx, snap.SagaID, domain.StepKeyAcceptDelivery, domain.SessionStatusCompleted, "order_completed", "Your order has been completed.", "success", snap, snap.BuyerID, true)
		_ = s.publishLifecycleMail(ctx, snap, domain.SessionStatusCompleted, snap.BuyerID, "order_completed_buyer", "order_completed", "Your order has been completed.")
		_ = s.publishLifecycleMail(ctx, snap, domain.SessionStatusCompleted, snap.SellerID, "order_completed_seller", "order_completed", "The buyer accepted the delivery and funds were released.")
		_ = s.publishChatClose(ctx, snap.OrderID, "order_completed")
		s.log.Info("order completed",
			logging.Operation("order.completed"),
			logging.String("order_id", snap.OrderID),
			logging.String("gig_id", snap.GigID),
			logging.String("buyer_id", snap.BuyerID),
			logging.String("seller_id", snap.SellerID),
		)
		_ = s.publishReviewPrompt(ctx, snap)
		return nil
	}
	releaseResult, err := s.payments.ReleaseFunds(ctx, ReleaseFundsCommand{
		OrderID:        snap.OrderID,
		PaymentID:      snap.PaymentIntentID,
		SellerUserID:   snap.SellerID,
		AmountCents:    snap.PriceCents,
		Currency:       snap.Currency,
		IdempotencyKey: snap.SagaID + ":payment.release_funds",
		RequestedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	if _, err := s.orders.MarkOrderCompleted(ctx, MarkOrderCompletedCommand{OrderID: snap.OrderID, PaymentReleaseID: releaseResult.PaymentReleaseID, RequestedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		return err
	}
	_ = s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusCompleted)
	if err := s.publishOrderLifecycle(ctx, snap.SagaID, domain.StepKeyAcceptDelivery, domain.SessionStatusCompleted, "order_completed", "Your order has been completed.", "success", snap, snap.BuyerID, true); err != nil {
		return err
	}
	if err := s.publishLifecycleMail(ctx, snap, domain.SessionStatusCompleted, snap.BuyerID, "order_completed_buyer", "order_completed", "Your order has been completed."); err != nil {
		return err
	}
	if err := s.publishLifecycleMail(ctx, snap, domain.SessionStatusCompleted, snap.SellerID, "order_completed_seller", "order_completed", "The buyer accepted the delivery and funds were released."); err != nil {
		return err
	}
	if err := s.publishChatClose(ctx, snap.OrderID, "order_completed"); err != nil {
		return err
	}
	s.log.Info("order completed",
		logging.Operation("order.completed"),
		logging.String("order_id", snap.OrderID),
		logging.String("gig_id", snap.GigID),
		logging.String("buyer_id", snap.BuyerID),
		logging.String("seller_id", snap.SellerID),
	)
	if err := s.publishReviewPrompt(ctx, snap); err != nil {
		return err
	}
	return nil
}

func (s *service) RequestRevision(ctx context.Context, cmd RequestRevisionCommand) (*RequestRevisionResult, error) {
	if cmd.RequestedAt == "" {
		cmd.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	snap, err := s.orders.GetOrderLifecycleSnapshot(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(snap.BuyerID) != strings.TrimSpace(cmd.BuyerID) {
		return nil, ErrOrderNotOwned
	}
	if snap.Status != domain.SessionStatusDelivered {
		return nil, ErrOrderNotRevisionable
	}
	if _, err := s.orders.RequestRevision(ctx, RequestRevisionCommand{
		OrderID:     cmd.OrderID,
		BuyerID:     cmd.BuyerID,
		Reason:      cmd.Reason,
		RequestedAt: cmd.RequestedAt,
	}); err != nil {
		return nil, err
	}
	_ = s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusRevisionRequested)
	if err := s.publishOrderLifecycle(ctx, snap.SagaID, domain.StepKeyRequestRevision, domain.SessionStatusRevisionRequested, "order_revision_requested", "The buyer requested a revision.", "warning", snap, snap.SellerID, false); err != nil {
		return nil, err
	}
	if err := s.publishLifecycleMail(ctx, snap, domain.SessionStatusRevisionRequested, snap.SellerID, "order_revision_requested_seller", "order_revision_requested", "The buyer requested a revision."); err != nil {
		return nil, err
	}
	return &RequestRevisionResult{OrderID: snap.OrderID, Status: domain.SessionStatusRevisionRequested, CurrentStep: "seller_revision_pending"}, nil
}

func (s *service) OpenDispute(ctx context.Context, cmd OpenDisputeCommand) (*OpenDisputeResult, error) {
	if cmd.RequestedAt == "" {
		cmd.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	snap, err := s.orders.GetOrderLifecycleSnapshot(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	actorID := strings.TrimSpace(cmd.ActorID)
	if actorID != strings.TrimSpace(snap.BuyerID) && actorID != strings.TrimSpace(snap.SellerID) {
		return nil, ErrOrderNotOwned
	}
	disputeType := ""
	switch {
	case actorID == strings.TrimSpace(snap.BuyerID) && snap.Status == domain.SessionStatusFunded:
		disputeType = "buyer_cancel_before_delivery"
	case actorID == strings.TrimSpace(snap.SellerID) && snap.Status == domain.SessionStatusFunded:
		disputeType = "seller_cancel_before_delivery"
	case actorID == strings.TrimSpace(snap.BuyerID) && (snap.Status == domain.SessionStatusDelivered || snap.Status == domain.SessionStatusRevisionRequested):
		disputeType = "buyer_dispute_after_delivery"
	default:
		return nil, ErrOrderNotDisputable
	}
	if _, err := s.orders.OpenDispute(ctx, OpenDisputeCommand{
		OrderID:     cmd.OrderID,
		ActorID:     actorID,
		DisputeType: disputeType,
		Reason:      cmd.Reason,
		RequestedAt: cmd.RequestedAt,
	}); err != nil {
		return nil, err
	}
	_ = s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusDisputed)
	if err := s.publishOrderLifecycleToParticipants(ctx, snap.SagaID, domain.StepKeyOpenDispute, domain.SessionStatusDisputed, "order_disputed", "The order is under review.", "warning", snap, snap.BuyerID, snap.SellerID); err != nil {
		s.log.Warn("failed to publish order dispute lifecycle notification",
			logging.Operation("order.dispute.notification"),
			logging.String("order_id", snap.OrderID),
			logging.Err(err),
		)
	}
	if err := s.publishLifecycleMail(ctx, snap, domain.SessionStatusDisputed, snap.BuyerID, "order_disputed_buyer", "order_disputed", "Your dispute was received."); err != nil {
		s.log.Warn("failed to publish buyer dispute mail",
			logging.Operation("order.dispute.mail"),
			logging.String("order_id", snap.OrderID),
			logging.String("user_id", snap.BuyerID),
			logging.Err(err),
		)
	}
	if err := s.publishLifecycleMail(ctx, snap, domain.SessionStatusDisputed, snap.SellerID, "order_disputed_seller", "order_disputed", "The buyer opened a dispute."); err != nil {
		s.log.Warn("failed to publish seller dispute mail",
			logging.Operation("order.dispute.mail"),
			logging.String("order_id", snap.OrderID),
			logging.String("user_id", snap.SellerID),
			logging.Err(err),
		)
	}
	return &OpenDisputeResult{OrderID: snap.OrderID, Status: domain.SessionStatusDisputed, CurrentStep: "resolution_pending"}, nil
}

func (s *service) ResolveDispute(ctx context.Context, cmd SettleDisputeCommand) (*SettleDisputeResult, error) {
	if cmd.RequestedAt == "" {
		cmd.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if cmd.FreelancerPercentage < 0 || cmd.CustomerPercentage < 0 || cmd.FreelancerPercentage+cmd.CustomerPercentage != 100 {
		return nil, ErrInvalidDisputeSplit
	}
	snap, err := s.orders.GetOrderLifecycleSnapshot(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	switch snap.Status {
	case domain.SessionStatusDisputed, domain.SessionStatusRevisionRequested, domain.SessionStatusDelivered:
	default:
		return nil, ErrOrderNotDisputable
	}
	releaseState, err := s.payments.GetReleaseByOrderID(ctx, snap.OrderID)
	if err == nil && releaseState != nil && releaseState.Status == "released" {
		if _, err := s.orders.MarkDisputeResolved(ctx, MarkDisputeResolvedCommand{OrderID: snap.OrderID, PaymentReleaseID: releaseState.PaymentReleaseID, RequestedAt: cmd.RequestedAt}); err != nil {
			return nil, err
		}
		_ = s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusDisputeResolved)
		_ = s.publishChatClose(ctx, snap.OrderID, "dispute_resolved")
		return &SettleDisputeResult{
			OrderID:          snap.OrderID,
			PaymentReleaseID: releaseState.PaymentReleaseID,
			StripeTransferID: releaseState.StripeTransferID,
			Status:           domain.SessionStatusDisputeResolved,
			CurrentStep:      "resolution_completed",
			OccurredAt:       releaseState.OccurredAt,
		}, nil
	}
	settlement, err := s.payments.SettleDispute(ctx, SettleDisputeCommand{
		OrderID:              snap.OrderID,
		AdminUserID:          cmd.AdminUserID,
		PaymentID:            snap.PaymentIntentID,
		SellerUserID:         snap.SellerID,
		AmountCents:          snap.PriceCents,
		Currency:             snap.Currency,
		FreelancerPercentage: cmd.FreelancerPercentage,
		CustomerPercentage:   cmd.CustomerPercentage,
		IdempotencyKey:       cmd.IdempotencyKey,
		Reason:               cmd.Reason,
		RequestedAt:          cmd.RequestedAt,
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.orders.MarkDisputeResolved(ctx, MarkDisputeResolvedCommand{OrderID: snap.OrderID, PaymentReleaseID: settlement.PaymentReleaseID, RequestedAt: cmd.RequestedAt}); err != nil {
		return nil, err
	}
	_ = s.sessions.UpdateStatus(ctx, snap.SagaID, domain.SessionStatusDisputeResolved)
	if err := s.publishOrderLifecycleToParticipants(ctx, snap.SagaID, domain.StepKeyResolveDispute, domain.SessionStatusDisputeResolved, "order_dispute_resolved", "The dispute was resolved.", "success", snap, snap.BuyerID, snap.SellerID); err != nil {
		s.log.Warn("failed to publish dispute resolution lifecycle notification",
			logging.Operation("order.dispute_resolution.notification"),
			logging.String("order_id", snap.OrderID),
			logging.Err(err),
		)
	}
	if err := s.publishLifecycleMail(ctx, snap, domain.SessionStatusDisputeResolved, snap.BuyerID, "order_dispute_resolved_buyer", "order_dispute_resolved", "The dispute was resolved."); err != nil {
		s.log.Warn("failed to publish buyer dispute resolution mail",
			logging.Operation("order.dispute_resolution.mail"),
			logging.String("order_id", snap.OrderID),
			logging.String("user_id", snap.BuyerID),
			logging.Err(err),
		)
	}
	if err := s.publishLifecycleMail(ctx, snap, domain.SessionStatusDisputeResolved, snap.SellerID, "order_dispute_resolved_seller", "order_dispute_resolved", "The dispute was resolved."); err != nil {
		s.log.Warn("failed to publish seller dispute resolution mail",
			logging.Operation("order.dispute_resolution.mail"),
			logging.String("order_id", snap.OrderID),
			logging.String("user_id", snap.SellerID),
			logging.Err(err),
		)
	}
	if err := s.publishChatClose(ctx, snap.OrderID, "dispute_resolved"); err != nil {
		s.log.Warn("failed to publish chat close after dispute resolution",
			logging.Operation("order.dispute_resolution.chat_close"),
			logging.String("order_id", snap.OrderID),
			logging.Err(err),
		)
	}
	return &SettleDisputeResult{
		OrderID:          snap.OrderID,
		PaymentReleaseID: settlement.PaymentReleaseID,
		StripeTransferID: settlement.StripeTransferID,
		StripeRefundID:   settlement.StripeRefundID,
		Status:           domain.SessionStatusDisputeResolved,
		CurrentStep:      "resolution_completed",
		OccurredAt:       settlement.OccurredAt,
	}, nil
}

func (s *service) HandleOrderCreateResult(ctx context.Context, res OrderSagaResult) error {
	if err := s.resolveSagaID(ctx, &res.SagaID, res.OrderID); err != nil {
		return err
	}
	if err := s.steps.UpdateStatus(ctx, res.SagaID, domain.StepKeyCreateOrder, stepStatusFromResult(res.Status)); err != nil {
		return err
	}
	if res.Status != "success" {
		_ = s.sessions.UpdateStatus(ctx, res.SagaID, domain.SessionStatusFailed)
		return s.publishOrderFailedNotification(ctx, res.SagaID, "order.create", res.Error)
	}
	if err := s.sessions.UpdateStatus(ctx, res.SagaID, domain.SessionStatusPendingPayment); err != nil {
		return err
	}
	session, err := s.sessions.GetByID(ctx, res.SagaID)
	if err != nil {
		return err
	}
	paymentPayload, err := protojson.Marshal(&paymentflowv1.PaymentIntentCommand{
		SagaId:          session.SagaID,
		OrderId:         session.OrderID,
		PaymentIntentId: uuid.Must(uuid.NewV7()).String(),
		AmountCents:     session.PriceCents,
		Currency:        session.Currency,
		Provider:        "stripe",
		IdempotencyKey:  session.SagaID + ":payment.intent",
		RequestedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return ErrPublishCommand
	}
	if err := s.broker.Publish(ctx, s.cfg.PaymentIntentSubject, paymentPayload); err != nil {
		return err
	}
	return s.publishOrderAcceptedNotification(ctx, res.SagaID)
}

func (s *service) HandlePaymentIntentResult(ctx context.Context, res PaymentIntentResult) error {
	if err := s.resolveSagaID(ctx, &res.SagaID, res.OrderID); err != nil {
		return err
	}
	if err := s.steps.UpdateStatus(ctx, res.SagaID, domain.StepKeyCreatePaymentIntent, stepStatusFromResult(res.Status)); err != nil {
		return err
	}
	if res.Status != "success" && res.Status != "intent_created" {
		_ = s.sessions.UpdateStatus(ctx, res.SagaID, domain.SessionStatusFailed)
		return s.publishOrderFailedNotification(ctx, res.SagaID, "payment.intent", res.Error)
	}
	if _, err := s.orders.MarkPaymentPending(ctx, MarkPaymentPendingCommand{
		OrderID:         res.OrderID,
		PaymentIntentID: res.PaymentIntentID,
		CheckoutURL:     res.CheckoutURL,
		RequestedAt:     res.OccurredAt,
	}); err != nil {
		return err
	}
	_ = s.sessions.UpdateStatus(ctx, res.SagaID, domain.SessionStatusPendingPayment)
	return nil
}

func (s *service) HandlePaymentStatus(ctx context.Context, evt PaymentStatusEvent) error {
	if err := s.resolveSagaID(ctx, &evt.SagaID, evt.OrderID); err != nil {
		return err
	}
	if err := s.steps.UpdateStatus(ctx, evt.SagaID, domain.StepKeyAwaitPaymentWebhook, stepStatusFromStatus(evt.Status)); err != nil {
		return err
	}
	if evt.Status == "failed" || evt.Status == "payment.order_payment_failed" {
		_ = s.sessions.UpdateStatus(ctx, evt.SagaID, domain.SessionStatusFailed)
		if _, err := s.orders.MarkPaymentFailed(ctx, MarkPaymentFailedCommand{OrderID: evt.OrderID, Reason: evt.Error}); err != nil {
			return err
		}
		if err := s.publishOrderFailedNotification(ctx, evt.SagaID, "await_payment_webhook", evt.Error); err != nil {
			return err
		}
		return s.publishOrderFailedEvent(ctx, evt.SagaID, evt.OrderID, evt.Error)
	}
	_ = s.sessions.UpdateStatus(ctx, evt.SagaID, domain.SessionStatusFunded)
	snap, err := s.orders.GetOrderLifecycleSnapshot(ctx, evt.OrderID)
	if err != nil {
		return err
	}
	if _, err := s.orders.MarkOrderFunded(ctx, MarkOrderFundedCommand{OrderID: evt.OrderID, PaymentIntentID: evt.PaymentIntentID, RequestedAt: evt.OccurredAt}); err != nil {
		return err
	}
	if err := s.publishOrderConfirmedNotification(ctx, evt.SagaID, evt.PaymentIntentID); err != nil {
		return err
	}
	if err := s.publishOrderFundedEvent(ctx, evt.SagaID, evt.OrderID, snap.GigID, evt.OccurredAt); err != nil {
		return err
	}
	return s.sendReceiptEmail(ctx, evt.SagaID)
}

func stepStatusFromResult(status string) string {
	if status == "success" || status == "intent_created" {
		return domain.StepStatusCompleted
	}
	return domain.StepStatusFailed
}

func stepStatusFromStatus(status string) string {
	if status == "success" || status == "captured" || status == "paid" || status == "payment.order_payment_succeeded" {
		return domain.StepStatusCompleted
	}
	return domain.StepStatusFailed
}

func (s *service) sendReceiptEmail(ctx context.Context, sagaID string) error {
	session, err := s.sessions.GetByID(ctx, sagaID)
	if err != nil {
		return err
	}
	step, err := s.steps.GetByKey(ctx, sagaID, domain.StepKeySendReceipt)
	switch {
	case err == nil && step.Status == domain.StepStatusCompleted:
		return nil
	case err == nil:
		if err := s.steps.UpdateStatus(ctx, sagaID, domain.StepKeySendReceipt, domain.StepStatusInProgress); err != nil {
			return err
		}
	case err == domain.ErrStepNotFound:
		if _, err := s.steps.Create(ctx, domain.Step{SagaID: sagaID, StepKey: domain.StepKeySendReceipt, Status: domain.StepStatusInProgress}); err != nil {
			return err
		}
	default:
		return err
	}
	priceDisplay := fmt.Sprintf("%s %d.%02d", session.Currency, session.PriceCents/100, session.PriceCents%100)
	payload, err := s.mapr.marshal(MailSendCommand{
		SessionID:     session.SagaID,
		ClientID:      session.OrderID,
		UserID:        session.BuyerID,
		RequestID:     session.OrderID,
		CorrelationID: session.SagaID,
		MessageType:   "order_receipt",
		To:            session.BuyerEmail,
		Data: map[string]any{
			"buyer_email":           session.BuyerEmail,
			"order_id":              session.OrderID,
			"gig_title":             session.GigTitle,
			"package_tier":          session.PackageTier,
			"package_description":   session.PackageDescription,
			"package_delivery_days": session.PackageDeliveryDays,
			"price_cents":           session.PriceCents,
			"price_display":         priceDisplay,
			"currency":              session.Currency,
			"status":                session.Status,
		},
	})
	if err != nil {
		return err
	}
	if err := s.broker.Publish(ctx, s.cfg.MailSendSubject, payload); err != nil {
		return err
	}
	if err := s.steps.UpdateStatus(ctx, sagaID, domain.StepKeySendReceipt, domain.StepStatusCompleted); err != nil {
		return err
	}
	return nil
}

func (s *service) publishOrderLifecycle(ctx context.Context, sagaID, stepKey, orderStatus, kind, message, severity string, snap *OrderLifecycleSnapshot, userID string, isBuyer bool) error {
	step, err := s.ensureNotificationStep(ctx, sagaID, stepKey)
	if err != nil {
		return err
	}
	if step.Status == domain.StepStatusCompleted {
		return nil
	}
	if snap == nil {
		return ErrInvalidOrderSnapshot
	}
	priceDisplay := fmt.Sprintf("%s %d.%02d", snap.Currency, snap.PriceCents/100, snap.PriceCents%100)
	payload, err := s.mapr.marshal(OrderLifecycleNotification{
		RealtimeNotificationMetadata: RealtimeNotificationMetadata{
			SagaID:        snap.SagaID,
			OrderID:       snap.OrderID,
			UserID:        userID,
			CorrelationID: snap.SagaID,
			DedupeKey:     sagaID + ":" + stepKey,
		},
		Kind:               kind,
		Title:              titleForLifecycleKind(kind),
		Message:            message,
		Severity:           severity,
		OrderStatus:        orderStatus,
		GigTitle:           snap.GigTitle,
		PackageTier:        snap.PackageTitle,
		PackageDescription: snap.PackageDescription,
		PriceDisplay:       priceDisplay,
	})
	if err != nil {
		return ErrPublishCommand
	}
	if err := s.publishUserRealtimeDelivery(ctx, userID, payload); err != nil {
		return err
	}
	if err := s.publishLifecycleEvent(ctx, sagaID, snap.OrderID, stepKey, orderStatus, ""); err != nil {
		return err
	}
	if err := s.steps.UpdateStatus(ctx, sagaID, stepKey, domain.StepStatusCompleted); err != nil {
		return err
	}
	return nil
}

func (s *service) publishOrderLifecycleToParticipants(ctx context.Context, sagaID, stepKey, orderStatus, kind, message, severity string, snap *OrderLifecycleSnapshot, userIDs ...string) error {
	step, err := s.ensureNotificationStep(ctx, sagaID, stepKey)
	if err != nil {
		return err
	}
	if step.Status == domain.StepStatusCompleted {
		return nil
	}
	if snap == nil {
		return ErrInvalidOrderSnapshot
	}
	priceDisplay := fmt.Sprintf("%s %d.%02d", snap.Currency, snap.PriceCents/100, snap.PriceCents%100)
	seen := make(map[string]struct{}, len(userIDs))
	for _, userID := range userIDs {
		userID = strings.TrimSpace(userID)
		if userID == "" {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		payload, err := s.mapr.marshal(OrderLifecycleNotification{
			RealtimeNotificationMetadata: RealtimeNotificationMetadata{
				SagaID:        snap.SagaID,
				OrderID:       snap.OrderID,
				UserID:        userID,
				CorrelationID: snap.SagaID,
				DedupeKey:     sagaID + ":" + stepKey + ":" + userID,
			},
			Kind:               kind,
			Title:              titleForLifecycleKind(kind),
			Message:            message,
			Severity:           severity,
			OrderStatus:        orderStatus,
			GigTitle:           snap.GigTitle,
			PackageTier:        snap.PackageTitle,
			PackageDescription: snap.PackageDescription,
			PriceDisplay:       priceDisplay,
		})
		if err != nil {
			return ErrPublishCommand
		}
		if err := s.publishUserRealtimeDelivery(ctx, userID, payload); err != nil {
			return err
		}
	}
	if err := s.publishLifecycleEvent(ctx, sagaID, snap.OrderID, stepKey, orderStatus, ""); err != nil {
		return err
	}
	if err := s.steps.UpdateStatus(ctx, sagaID, stepKey, domain.StepStatusCompleted); err != nil {
		return err
	}
	return nil
}

func (s *service) publishReviewPrompt(ctx context.Context, snap *OrderLifecycleSnapshot) error {
	step, err := s.ensureNotificationStep(ctx, snap.SagaID, domain.StepKeyRealtimeReviewPrompt)
	if err != nil {
		return err
	}
	if step.Status == domain.StepStatusCompleted {
		return nil
	}
	payload, err := s.mapr.marshal(ReviewPromptNotification{
		RealtimeNotificationMetadata: RealtimeNotificationMetadata{
			SagaID:        snap.SagaID,
			OrderID:       snap.OrderID,
			UserID:        snap.BuyerID,
			CorrelationID: snap.SagaID,
			DedupeKey:     snap.SagaID + ":" + domain.StepKeyRealtimeReviewPrompt,
		},
		Kind:        "review_prompt",
		Title:       "Leave a review",
		Message:     "Your order is complete. You can now leave a review for the gig.",
		Severity:    "info",
		ActionLabel: "Leave review",
	})
	if err != nil {
		return ErrPublishCommand
	}
	if err := s.publishUserRealtimeDelivery(ctx, snap.BuyerID, payload); err != nil {
		return err
	}
	return s.steps.UpdateStatus(ctx, snap.SagaID, domain.StepKeyRealtimeReviewPrompt, domain.StepStatusCompleted)
}

func titleForLifecycleKind(kind string) string {
	switch kind {
	case "order_delivered":
		return "Delivery submitted"
	case "order_revision_requested":
		return "Revision requested"
	case "order_disputed":
		return "Dispute opened"
	case "order_dispute_resolved":
		return "Dispute resolved"
	case "order_completed":
		return "Order completed"
	case "order_release_failed":
		return "Payout release failed"
	default:
		return "Order update"
	}
}

type ReviewPromptNotification struct {
	RealtimeNotificationMetadata
	Kind        string `json:"kind"`
	Title       string `json:"title"`
	Message     string `json:"message"`
	Severity    string `json:"severity"`
	ActionLabel string `json:"action_label"`
	ActionURL   string `json:"action_url,omitempty"`
}

func (s *service) publishLifecycleMail(ctx context.Context, snap *OrderLifecycleSnapshot, orderStatus, userID, messageType, kind, message string) error {
	email, err := s.auth.GetEmailByUserID(ctx, userID)
	if err != nil {
		return err
	}
	stepKey := domain.StepKeySendLifecycleMail + ":" + messageType
	step, err := s.ensureNotificationStep(ctx, snap.SagaID, stepKey)
	if err != nil {
		return err
	}
	if step.Status == domain.StepStatusCompleted {
		return nil
	}
	priceDisplay := fmt.Sprintf("%s %d.%02d", snap.Currency, snap.PriceCents/100, snap.PriceCents%100)
	payload, err := s.mapr.marshal(MailSendCommand{
		SessionID:     snap.SagaID,
		ClientID:      snap.OrderID,
		UserID:        userID,
		RequestID:     snap.OrderID,
		CorrelationID: snap.SagaID,
		MessageType:   messageType,
		To:            email,
		Data: map[string]any{
			"recipient_email":       email,
			"order_id":              snap.OrderID,
			"order_status":          orderStatus,
			"event_kind":            kind,
			"gig_title":             snap.GigTitle,
			"package_tier":          snap.PackageTitle,
			"package_description":   snap.PackageDescription,
			"package_delivery_days": 0,
			"revision_count":        snap.RevisionCountSnapshot,
			"price_cents":           snap.PriceCents,
			"price_display":         priceDisplay,
			"currency":              snap.Currency,
			"message":               message,
		},
	})
	if err != nil {
		return err
	}
	if err := s.broker.Publish(ctx, s.cfg.MailSendSubject, payload); err != nil {
		return err
	}
	return s.steps.UpdateStatus(ctx, snap.SagaID, stepKey, domain.StepStatusCompleted)
}

func (s *service) publishUserRealtimeDelivery(ctx context.Context, userID string, payload []byte) error {
	delivery, err := s.mapr.marshal(realtimeDeliveryMessage{
		UserID:        userID,
		DeliveryScope: "user",
		Type:          "order.realtime",
		Payload:       payload,
	})
	if err != nil {
		return ErrPublishCommand
	}
	return s.broker.Publish(ctx, realtimeSubject, delivery)
}

func (s *service) publishLifecycleEvent(ctx context.Context, sagaID, orderID, stepKey, status, reason string) error {
	subject := ""
	switch stepKey {
	case domain.StepKeyDeliverOrder:
		subject = s.cfg.OrderDeliveredSubject
	case domain.StepKeyRequestRevision:
		subject = s.cfg.OrderRevisionRequestedSubject
	case domain.StepKeyOpenDispute:
		subject = s.cfg.OrderDisputedSubject
	case domain.StepKeyResolveDispute:
		subject = s.cfg.OrderDisputeResolvedSubject
	case domain.StepKeyAcceptDelivery:
		subject = s.cfg.OrderCompletedSubject
	case domain.StepKeyReleaseFunds:
		subject = s.cfg.OrderReleaseFailedSubject
	default:
		return nil
	}
	if strings.TrimSpace(subject) == "" {
		return nil
	}
	payload, err := s.mapr.marshal(OrderSagaResult{
		SagaID:     sagaID,
		OrderID:    orderID,
		Status:     status,
		Error:      reason,
		Operation:  stepKey,
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return ErrPublishCommand
	}
	return s.broker.Publish(ctx, subject, payload)
}

func (s *service) publishChatCreate(ctx context.Context, orderID, buyerID, sellerID string) error {
	if strings.TrimSpace(s.cfg.ChatCreateSubject) == "" {
		return nil
	}
	payload, err := s.mapr.marshal(ChatCreateCommand{
		OrderID:  orderID,
		BuyerID:  buyerID,
		SellerID: sellerID,
	})
	if err != nil {
		return ErrPublishCommand
	}
	return s.broker.Publish(ctx, s.cfg.ChatCreateSubject, payload)
}

func (s *service) publishChatClose(ctx context.Context, orderID, closeReason string) error {
	if strings.TrimSpace(s.cfg.ChatCloseSubject) == "" {
		return nil
	}
	payload, err := s.mapr.marshal(ChatCloseCommand{
		OrderID:     orderID,
		CloseReason: closeReason,
	})
	if err != nil {
		return ErrPublishCommand
	}
	return s.broker.Publish(ctx, s.cfg.ChatCloseSubject, payload)
}

func (s *service) resolveSagaID(ctx context.Context, sagaID *string, orderID string) error {
	if strings.TrimSpace(*sagaID) != "" || strings.TrimSpace(orderID) == "" {
		return nil
	}
	session, err := s.sessions.GetByOrderID(ctx, orderID)
	if err != nil {
		return err
	}
	*sagaID = session.SagaID
	return nil
}

func (s *service) publishOrderAcceptedNotification(ctx context.Context, sagaID string) error {
	step, err := s.ensureNotificationStep(ctx, sagaID, domain.StepKeyRealtimeOrderAccepted)
	if err != nil {
		return err
	}
	if step.Status == domain.StepStatusCompleted {
		return nil
	}
	session, err := s.sessions.GetByID(ctx, sagaID)
	if err != nil {
		return err
	}
	priceDisplay := fmt.Sprintf("%s %d.%02d", session.Currency, session.PriceCents/100, session.PriceCents%100)
	payload, err := s.mapr.marshal(OrderAcceptedNotification{
		RealtimeNotificationMetadata: RealtimeNotificationMetadata{
			SagaID:        session.SagaID,
			OrderID:       session.OrderID,
			UserID:        session.BuyerID,
			CorrelationID: session.SagaID,
			DedupeKey:     sagaID + ":realtime.order.accepted",
		},
		Kind:               "order_accepted",
		Title:              "Order accepted",
		Message:            "We received your order and started processing it.",
		Severity:           "info",
		OrderStatus:        session.Status,
		GigTitle:           session.GigTitle,
		PackageTier:        session.PackageTier,
		PackageDescription: session.PackageDescription,
		PriceDisplay:       priceDisplay,
		ActionLabel:        "View order",
		ActionURL:          "",
	})
	if err != nil {
		return ErrPublishCommand
	}
	if err := s.publishRealtimeDelivery(ctx, payload, session.BuyerID); err != nil {
		return err
	}
	return s.steps.UpdateStatus(ctx, sagaID, domain.StepKeyRealtimeOrderAccepted, domain.StepStatusCompleted)
}

func (s *service) publishPaymentReadyNotification(ctx context.Context, sagaID, checkoutURL, paymentIntentID string) error {
	step, err := s.ensureNotificationStep(ctx, sagaID, domain.StepKeyRealtimePaymentReady)
	if err != nil {
		return err
	}
	if step.Status == domain.StepStatusCompleted {
		return nil
	}
	session, err := s.sessions.GetByID(ctx, sagaID)
	if err != nil {
		return err
	}
	priceDisplay := fmt.Sprintf("%s %d.%02d", session.Currency, session.PriceCents/100, session.PriceCents%100)
	payload, err := s.mapr.marshal(PaymentReadyNotification{
		RealtimeNotificationMetadata: RealtimeNotificationMetadata{
			SagaID:        session.SagaID,
			OrderID:       session.OrderID,
			UserID:        session.BuyerID,
			CorrelationID: session.SagaID,
			DedupeKey:     sagaID + ":realtime.payment.ready",
		},
		Kind:               "payment_ready",
		Title:              "Payment is ready",
		Message:            "Your checkout link is ready. Complete payment to confirm the order.",
		Severity:           "info",
		CheckoutURL:        checkoutURL,
		PaymentIntentID:    paymentIntentID,
		GigTitle:           session.GigTitle,
		PackageTier:        session.PackageTier,
		PackageDescription: session.PackageDescription,
		PriceDisplay:       priceDisplay,
		ActionLabel:        "Pay now",
		ActionURL:          checkoutURL,
	})
	if err != nil {
		return ErrPublishCommand
	}
	if err := s.publishRealtimeDelivery(ctx, payload, session.BuyerID); err != nil {
		return err
	}
	return s.steps.UpdateStatus(ctx, sagaID, domain.StepKeyRealtimePaymentReady, domain.StepStatusCompleted)
}

func (s *service) publishOrderConfirmedNotification(ctx context.Context, sagaID, paymentIntentID string) error {
	step, err := s.ensureNotificationStep(ctx, sagaID, domain.StepKeyRealtimeOrderConfirmed)
	if err != nil {
		return err
	}
	if step.Status == domain.StepStatusCompleted {
		return nil
	}
	session, err := s.sessions.GetByID(ctx, sagaID)
	if err != nil {
		return err
	}
	priceDisplay := fmt.Sprintf("%s %d.%02d", session.Currency, session.PriceCents/100, session.PriceCents%100)
	payload, err := s.mapr.marshal(OrderConfirmedNotification{
		RealtimeNotificationMetadata: RealtimeNotificationMetadata{
			SagaID:        session.SagaID,
			OrderID:       session.OrderID,
			UserID:        session.BuyerID,
			CorrelationID: session.SagaID,
			DedupeKey:     sagaID + ":realtime.order.confirmed",
		},
		Kind:                "order_confirmed",
		Title:               "Order confirmed",
		Message:             "Your payment was captured and the order is confirmed.",
		Severity:            "success",
		PaymentIntentID:     paymentIntentID,
		GigTitle:            session.GigTitle,
		PackageTier:         session.PackageTier,
		PackageDescription:  session.PackageDescription,
		PackageDeliveryDays: session.PackageDeliveryDays,
		PriceDisplay:        priceDisplay,
		ActionLabel:         "View receipt",
	})
	if err != nil {
		return ErrPublishCommand
	}
	if err := s.publishRealtimeDelivery(ctx, payload, session.BuyerID); err != nil {
		return err
	}
	return s.steps.UpdateStatus(ctx, sagaID, domain.StepKeyRealtimeOrderConfirmed, domain.StepStatusCompleted)
}

func (s *service) publishOrderFailedNotification(ctx context.Context, sagaID, failureStep, reason string) error {
	step, err := s.ensureNotificationStep(ctx, sagaID, domain.StepKeyRealtimeOrderFailed)
	if err != nil {
		return err
	}
	if step.Status == domain.StepStatusCompleted {
		return nil
	}
	session, err := s.sessions.GetByID(ctx, sagaID)
	if err != nil {
		return err
	}
	priceDisplay := fmt.Sprintf("%s %d.%02d", session.Currency, session.PriceCents/100, session.PriceCents%100)
	payload, err := s.mapr.marshal(OrderFailedNotification{
		RealtimeNotificationMetadata: RealtimeNotificationMetadata{
			SagaID:        session.SagaID,
			OrderID:       session.OrderID,
			UserID:        session.BuyerID,
			CorrelationID: session.SagaID,
			DedupeKey:     sagaID + ":realtime.order.failed",
		},
		Kind:               "order_failed",
		Title:              "Order failed",
		Message:            "We could not complete your order.",
		Severity:           "error",
		FailureStep:        failureStep,
		Reason:             reason,
		GigTitle:           session.GigTitle,
		PackageTier:        session.PackageTier,
		PackageDescription: session.PackageDescription,
		PriceDisplay:       priceDisplay,
		ActionLabel:        "Try again",
	})
	if err != nil {
		return ErrPublishCommand
	}
	if err := s.publishRealtimeDelivery(ctx, payload, session.BuyerID); err != nil {
		return err
	}
	return s.steps.UpdateStatus(ctx, sagaID, domain.StepKeyRealtimeOrderFailed, domain.StepStatusCompleted)
}

func (s *service) publishRealtimeDelivery(ctx context.Context, payload []byte, userID string) error {
	delivery, err := s.mapr.marshal(realtimeDeliveryMessage{
		DeliveryScope: "user",
		UserID:        userID,
		Type:          "order.realtime",
		Payload:       payload,
	})
	if err != nil {
		return ErrPublishCommand
	}
	return s.broker.Publish(ctx, realtimeSubject, delivery)
}

const realtimeSubject = "realtime"

func (s *service) publishOrderFundedEvent(ctx context.Context, sagaID, orderID, gigID, occurredAt string) error {
	payload, err := protojson.Marshal(&orderflowv1.OrderSagaResult{
		SagaId:     sagaID,
		OrderId:    orderID,
		GigId:      gigID,
		Status:     "success",
		Operation:  "payment_funded",
		OccurredAt: occurredAt,
	})
	if err != nil {
		return ErrPublishCommand
	}
	subject := s.cfg.OrderFundedSubject
	if subject == "" {
		subject = "order.funded"
	}
	return s.broker.Publish(ctx, subject, payload)
}

func (s *service) publishOrderFailedEvent(ctx context.Context, sagaID, orderID, reason string) error {
	payload, err := protojson.Marshal(&orderflowv1.OrderSagaResult{
		SagaId:     sagaID,
		OrderId:    orderID,
		Status:     "failed",
		Error:      reason,
		Operation:  "await_payment_webhook",
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return ErrPublishCommand
	}
	return s.broker.Publish(ctx, s.cfg.OrderFailSubject, payload)
}

func (s *service) ensureNotificationStep(ctx context.Context, sagaID, stepKey string) (*domain.Step, error) {
	step, err := s.steps.GetByKey(ctx, sagaID, stepKey)
	if err == nil {
		return step, nil
	}
	if err != domain.ErrStepNotFound {
		return nil, err
	}
	return s.steps.Create(ctx, domain.Step{SagaID: sagaID, StepKey: stepKey, Status: domain.StepStatusPending})
}
