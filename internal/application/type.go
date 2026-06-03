package application

import (
	"context"
	"encoding/json"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	"order-saga-service/internal/domain"
)

// Service owns the order saga orchestration over NATS.
type Service interface {
	Start(ctx context.Context, cmd OrderSagaCommand) error
	HandleOrderCreateResult(ctx context.Context, res OrderSagaResult) error
	HandlePaymentIntentResult(ctx context.Context, res PaymentIntentResult) error
	HandlePaymentStatus(ctx context.Context, evt PaymentStatusEvent) error
	HandleReleaseFunds(ctx context.Context, orderID string) error
	StartOrder(ctx context.Context, cmd StartOrderCommand) (*StartOrderResult, error)
	ConfirmOrder(ctx context.Context, cmd ConfirmOrderCommand) (*ConfirmOrderResult, error)
	SubmitRequirements(ctx context.Context, cmd SubmitRequirementsCommand) (*SubmitRequirementsResult, error)
	SubmitMessage(ctx context.Context, cmd SubmitMessageCommand) (*SubmitMessageResult, error)
	DeliverOrder(ctx context.Context, cmd DeliverOrderCommand) (*DeliverOrderResult, error)
	AcceptDelivery(ctx context.Context, cmd AcceptDeliveryCommand) (*AcceptDeliveryResult, error)
	RequestRevision(ctx context.Context, cmd RequestRevisionCommand) (*RequestRevisionResult, error)
	OpenDispute(ctx context.Context, cmd OpenDisputeCommand) (*OpenDisputeResult, error)
}

// GigSnapshotClient resolves the authoritative gig/package snapshot used to
// initialize the order wizard.
type GigSnapshotClient interface {
	GetOrderStartSnapshot(ctx context.Context, gigID, packageID string) (*OrderStartSnapshot, error)
}

// AuthQueryClient resolves auth-owned user data needed by the order saga.
type AuthQueryClient interface {
	GetEmailByUserID(ctx context.Context, userID string) (string, error)
	Close() error
}

// OrderWriteClient persists draft and lifecycle updates in order-service.
type OrderWriteClient interface {
	CreateDraftOrder(ctx context.Context, cmd CreateDraftOrderCommand) (*CreateDraftOrderResult, error)
	GetOrderPaymentSnapshot(ctx context.Context, orderID string) (*OrderPaymentSnapshot, error)
	GetOrderLifecycleSnapshot(ctx context.Context, orderID string) (*OrderLifecycleSnapshot, error)
	SaveRequirementAnswers(ctx context.Context, cmd SaveRequirementAnswersCommand) (*SaveRequirementAnswersResult, error)
	SaveBuyerInitialMessage(ctx context.Context, cmd SaveBuyerInitialMessageCommand) (*SaveBuyerInitialMessageResult, error)
	AttachFile(ctx context.Context, cmd AttachFileCommand) (*AttachFileResult, error)
	MarkPaymentPending(ctx context.Context, cmd MarkPaymentPendingCommand) (*MarkPaymentPendingResult, error)
	MarkOrderFunded(ctx context.Context, cmd MarkOrderFundedCommand) (*MarkOrderFundedResult, error)
	MarkPaymentFailed(ctx context.Context, cmd MarkPaymentFailedCommand) (*MarkPaymentFailedResult, error)
	SaveDelivery(ctx context.Context, cmd SaveDeliveryCommand) (*SaveDeliveryResult, error)
	MarkReleasePending(ctx context.Context, cmd MarkReleasePendingCommand) (*MarkReleasePendingResult, error)
	RequestRevision(ctx context.Context, cmd RequestRevisionCommand) (*RequestRevisionResult, error)
	OpenDispute(ctx context.Context, cmd OpenDisputeCommand) (*OpenDisputeResult, error)
	MarkOrderCompleted(ctx context.Context, cmd MarkOrderCompletedCommand) (*MarkOrderCompletedResult, error)
	MarkReleaseFailed(ctx context.Context, cmd MarkReleaseFailedCommand) (*MarkReleaseFailedResult, error)
	Close() error
}

// PaymentCheckoutClient creates checkout sessions in payment-service.
type PaymentCheckoutClient interface {
	GetConnectStatus(ctx context.Context, userID string) (*GetConnectStatusResult, error)
	CreateCheckoutSession(ctx context.Context, cmd CreateCheckoutSessionCommand) (*CreateCheckoutSessionResult, error)
	ReleaseFunds(ctx context.Context, cmd ReleaseFundsCommand) (*ReleaseFundsResult, error)
	GetReleaseByOrderID(ctx context.Context, orderID string) (*GetReleaseByOrderResult, error)
	Close() error
}

// GetConnectStatusResult normalizes payment-service connect state for the saga.
type GetConnectStatusResult struct {
	UserID          string
	StripeAccountID string
	Status          string
	DisabledReason  string
	OccurredAt      string
}

// EventBroker abstracts the NATS broker implementation.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error
	RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler MessageHandler) error
	Close()
}

// MessageHandler processes one broker payload.
type MessageHandler func(ctx context.Context, subject string, payload []byte) error

// Logger aliases the shared structured logger.
type Logger = logging.Logger

// OrderSagaCommand is the root command accepted by the order saga.
type OrderSagaCommand struct {
	SagaID               string `json:"saga_id"`
	OrderID              string `json:"order_id"`
	BuyerID              string `json:"buyer_id"`
	BuyerEmail           string `json:"buyer_email"`
	RealtimeConnectionID string `json:"realtime_connection_id,omitempty"`
	GigID                string `json:"gig_id"`
	PackageID            string `json:"package_id"`
	SellerUsername       string `json:"seller_username,omitempty"`
	IdempotencyKey       string `json:"idempotency_key"`
	RequestedAt          string `json:"requested_at"`
}

// StartOrderCommand is the synchronous order start command accepted by the
// checkout saga gRPC boundary.
type StartOrderCommand struct {
	SagaID               string
	OrderID              string
	BuyerID              string
	RealtimeConnectionID string
	GigID                string
	PackageID            string
	SellerUsername       string
	IdempotencyKey       string
	RequestedAt          string
}

// StartOrderResult returns the snapshot needed by the buyer to continue the
// checkout flow.
type StartOrderResult struct {
	SagaID   string
	OrderID  string
	Status   string
	Snapshot *OrderStartSnapshot
}

// ConfirmOrderCommand initiates the checkout session creation.
type ConfirmOrderCommand struct {
	SagaID               string
	OrderID              string
	BuyerID              string
	RealtimeConnectionID string
	IdempotencyKey       string
	RequestedAt          string
}

// ConfirmOrderResult returns the checkout URL and payment identifiers.
type ConfirmOrderResult struct {
	SagaID      string
	OrderID     string
	Status      string
	CheckoutURL string
	PaymentID   string
}

// SubmitRequirementsCommand stores buyer requirement answers.
type SubmitRequirementsCommand struct {
	OrderID     string
	BuyerID     string
	Answers     []OrderAnswer
	RequestedAt string
}

// SubmitRequirementsResult reports the updated wizard step.
type SubmitRequirementsResult struct {
	OrderID     string
	Status      string
	CurrentStep string
}

// SubmitMessageCommand stores the buyer initial message.
type SubmitMessageCommand struct {
	OrderID     string
	BuyerID     string
	Message     string
	RequestedAt string
}

// SubmitMessageResult reports the updated wizard step.
type SubmitMessageResult struct {
	OrderID     string
	Status      string
	CurrentStep string
}

// CreateDraftOrderCommand persists the authoritative draft snapshot in
// order-service.
type CreateDraftOrderCommand struct {
	SagaID              string
	OrderID             string
	BuyerID             string
	SellerID            string
	SellerUsername      string
	GigID               string
	GigTitle            string
	PackageID           string
	PackageTier         string
	PackageDescription  string
	PackageDeliveryDays int32
	PriceCents          int64
	Currency            string
	Questions           []OrderQuestionSnapshot
	IdempotencyKey      string
	RequestedAt         string
}

// OrderQuestionSnapshot captures one immutable question snapshot.
type OrderQuestionSnapshot struct {
	QuestionID  string
	Text        string
	Type        string
	Required    bool
	OptionsJSON string
	SortOrder   int32
}

// CreateDraftOrderResult reports the created draft order identifiers.
type CreateDraftOrderResult struct {
	OrderID string
	Status  string
}

// SaveRequirementAnswersCommand stores buyer answers on the order write model.
type SaveRequirementAnswersCommand struct {
	OrderID string
	Answers []OrderAnswer
}

// OrderAnswer stores one buyer answer snapshot.
type OrderAnswer struct {
	QuestionID string
	Value      string
}

// SaveRequirementAnswersResult reports the updated order status.
type SaveRequirementAnswersResult struct {
	OrderID string
	Status  string
}

// SaveBuyerInitialMessageCommand stores the buyer message on the order write model.
type SaveBuyerInitialMessageCommand struct {
	OrderID string
	Message string
}

// SaveBuyerInitialMessageResult reports the updated order status.
type SaveBuyerInitialMessageResult struct {
	OrderID string
	Status  string
}

// AttachFileCommand stores an attachment reference on the order write model.
type AttachFileCommand struct {
	OrderID      string
	AttachmentID string
}

// AttachFileResult reports the updated order status.
type AttachFileResult struct {
	OrderID string
	Status  string
}

// OrderPaymentSnapshot returns the payment-facing snapshot from order-service.
type OrderPaymentSnapshot struct {
	OrderID        string
	SagaID         string
	BuyerID        string
	SellerID       string
	SellerUsername string
	GigTitle       string
	PackageTitle   string
	PriceCents     int64
	Currency       string
	Status         string
}

// MarkPaymentPendingCommand records the payment checkout session.
type MarkPaymentPendingCommand struct {
	OrderID         string
	PaymentIntentID string
	CheckoutURL     string
	RequestedAt     string
}

// MarkPaymentPendingResult returns the updated order status.
type MarkPaymentPendingResult struct {
	OrderID string
	Status  string
}

// MarkOrderFundedCommand marks the order funded after webhook success.
type MarkOrderFundedCommand struct {
	OrderID         string
	PaymentIntentID string
	RequestedAt     string
}

// MarkOrderFundedResult returns the updated order status.
type MarkOrderFundedResult struct {
	OrderID string
	Status  string
}

// MarkPaymentFailedCommand marks the order failed after webhook failure.
type MarkPaymentFailedCommand struct {
	OrderID string
	Reason  string
}

// MarkPaymentFailedResult returns the updated order status.
type MarkPaymentFailedResult struct {
	OrderID string
	Status  string
}

// CreateCheckoutSessionCommand asks payment-service to create a Stripe
// checkout session.
type CreateCheckoutSessionCommand struct {
	SagaID         string
	OrderID        string
	BuyerID        string
	SellerID       string
	AmountCents    int64
	Currency       string
	Title          string
	IdempotencyKey string
	RequestedAt    string
}

// CreateCheckoutSessionResult returns the payment identifiers and checkout
// URL.
type CreateCheckoutSessionResult struct {
	PaymentIntentID string
	CheckoutURL     string
	Status          string
}

// ReleaseFundsCommand asks payment-service to transfer captured funds to the seller.
type ReleaseFundsCommand struct {
	OrderID        string
	PaymentID      string
	SellerUserID   string
	AmountCents    int64
	Currency       string
	IdempotencyKey string
	RequestedAt    string
}

// ReleaseFundsResult reports the payout outcome.
type ReleaseFundsResult struct {
	OrderID          string
	PaymentReleaseID string
	StripeTransferID string
	Status           string
	OccurredAt       string
}

// GetReleaseByOrderResult reports the persisted payout release state.
type GetReleaseByOrderResult struct {
	OrderID          string
	PaymentReleaseID string
	PaymentID        string
	SellerUserID     string
	AmountCents      int64
	Currency         string
	IdempotencyKey   string
	StripeTransferID string
	Status           string
	FailureReason    string
	OccurredAt       string
}

// OrderStartSnapshot contains the authoritative gig/package data required to
// create the order draft.
type OrderStartSnapshot struct {
	GigID              string
	PackageID          string
	SellerID           string
	SellerUsername     string
	GigTitle           string
	PackageTitle       string
	PackageDescription string
	PriceCents         int64
	Currency           string
	DeliveryDays       int32
	RevisionCount      int32
	GigPublished       bool
	PackageAvailable   bool
	Questions          []OrderStartQuestion
}

// OrderStartQuestion represents one requirement question in the start
// snapshot.
type OrderStartQuestion struct {
	ID        string
	Text      string
	SortOrder int32
}

// OrderLifecycleSnapshot returns the state required by delivery and completion transitions.
type OrderLifecycleSnapshot struct {
	OrderID               string
	SagaID                string
	BuyerID               string
	SellerID              string
	SellerUsername        string
	GigID                 string
	GigTitle              string
	PackageID             string
	PackageTitle          string
	PackageDescription    string
	PriceCents            int64
	Currency              string
	Status                string
	RevisionCountSnapshot int32
	RevisionCountUsed     int32
	BuyerResponseDeadline string
	PaymentIntentID       string
	PaymentReleaseID      string
	DeliveredAt           string
	CompletedAt           string
	DisputedAt            string
}

// OrderSagaResult records the outcome of an order step.
type OrderSagaResult struct {
	SagaID     string `json:"saga_id"`
	OrderID    string `json:"order_id"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
	Operation  string `json:"operation"`
	OccurredAt string `json:"occurred_at"`
}

// PaymentIntentResult records the payment-intent outcome.
type PaymentIntentResult struct {
	SagaID           string `json:"saga_id"`
	OrderID          string `json:"order_id"`
	PaymentIntentID  string `json:"payment_intent_id"`
	CheckoutURL      string `json:"checkout_url,omitempty"`
	ProviderIntentID string `json:"provider_intent_id,omitempty"`
	Status           string `json:"status"`
	Error            string `json:"error,omitempty"`
	OccurredAt       string `json:"occurred_at"`
}

// PaymentStatusEvent records webhook-normalized payment status.
type PaymentStatusEvent struct {
	SagaID          string `json:"saga_id"`
	OrderID         string `json:"order_id"`
	PaymentIntentID string `json:"payment_intent_id"`
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
	OccurredAt      string `json:"occurred_at"`
}

// RealtimeNotificationMetadata carries the routing fields shared by every
// realtime notification emitted by the order saga.
type RealtimeNotificationMetadata struct {
	SagaID        string `json:"saga_id"`
	OrderID       string `json:"order_id"`
	UserID        string `json:"user_id"`
	ClientID      string `json:"client_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	DedupeKey     string `json:"dedupe_key"`
}

// OrderAcceptedNotification is sent when the order is created and accepted by
// the saga.
type OrderAcceptedNotification struct {
	RealtimeNotificationMetadata
	Kind               string `json:"kind"`
	Title              string `json:"title"`
	Message            string `json:"message"`
	Severity           string `json:"severity"`
	OrderStatus        string `json:"order_status"`
	GigTitle           string `json:"gig_title"`
	PackageTier        string `json:"package_tier"`
	PackageDescription string `json:"package_description"`
	PriceDisplay       string `json:"price_display"`
	ActionLabel        string `json:"action_label,omitempty"`
	ActionURL          string `json:"action_url,omitempty"`
}

// PaymentReadyNotification is sent when the payment intent has been created
// and the buyer can complete checkout.
type PaymentReadyNotification struct {
	RealtimeNotificationMetadata
	Kind               string `json:"kind"`
	Title              string `json:"title"`
	Message            string `json:"message"`
	Severity           string `json:"severity"`
	CheckoutURL        string `json:"checkout_url"`
	PaymentIntentID    string `json:"payment_intent_id"`
	GigTitle           string `json:"gig_title"`
	PackageTier        string `json:"package_tier"`
	PackageDescription string `json:"package_description"`
	PriceDisplay       string `json:"price_display"`
	ActionLabel        string `json:"action_label,omitempty"`
	ActionURL          string `json:"action_url,omitempty"`
}

// OrderConfirmedNotification is sent after payment succeeds.
type OrderConfirmedNotification struct {
	RealtimeNotificationMetadata
	Kind                string `json:"kind"`
	Title               string `json:"title"`
	Message             string `json:"message"`
	Severity            string `json:"severity"`
	PaymentIntentID     string `json:"payment_intent_id"`
	GigTitle            string `json:"gig_title"`
	PackageTier         string `json:"package_tier"`
	PackageDescription  string `json:"package_description"`
	PackageDeliveryDays int32  `json:"package_delivery_days"`
	PriceDisplay        string `json:"price_display"`
	ActionLabel         string `json:"action_label,omitempty"`
	ActionURL           string `json:"action_url,omitempty"`
}

// OrderFailedNotification is sent when the saga cannot complete the order.
type OrderFailedNotification struct {
	RealtimeNotificationMetadata
	Kind               string `json:"kind"`
	Title              string `json:"title"`
	Message            string `json:"message"`
	Severity           string `json:"severity"`
	FailureStep        string `json:"failure_step"`
	Reason             string `json:"reason"`
	GigTitle           string `json:"gig_title"`
	PackageTier        string `json:"package_tier"`
	PackageDescription string `json:"package_description"`
	PriceDisplay       string `json:"price_display"`
	ActionLabel        string `json:"action_label,omitempty"`
	ActionURL          string `json:"action_url,omitempty"`
}

// OrderLifecycleNotification is a generic realtime notification for post-payment lifecycle events.
type OrderLifecycleNotification struct {
	RealtimeNotificationMetadata
	Kind               string `json:"kind"`
	Title              string `json:"title"`
	Message            string `json:"message"`
	Severity           string `json:"severity"`
	OrderStatus        string `json:"order_status"`
	GigTitle           string `json:"gig_title"`
	PackageTier        string `json:"package_tier"`
	PackageDescription string `json:"package_description"`
	PriceDisplay       string `json:"price_display"`
	ActionLabel        string `json:"action_label,omitempty"`
	ActionURL          string `json:"action_url,omitempty"`
}

// MailSendCommand represents the outbound email request sent to mail-service.
type MailSendCommand struct {
	SessionID     string `json:"session_id,omitempty"`
	ClientID      string `json:"client_id,omitempty"`
	UserID        string `json:"user_id,omitempty"`
	RequestID     string `json:"request_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	MessageType   string `json:"message_type"`
	To            string `json:"to"`
	Data          any    `json:"data"`
}

// MailReceiptData contains the structured payload for the receipt template.
type MailReceiptData struct {
	BuyerEmail          string `json:"buyer_email"`
	OrderID             string `json:"order_id"`
	GigTitle            string `json:"gig_title"`
	PackageTier         string `json:"package_tier"`
	PackageDescription  string `json:"package_description"`
	PackageDeliveryDays int32  `json:"package_delivery_days"`
	PriceCents          int64  `json:"price_cents"`
	PriceDisplay        string `json:"price_display"`
	Currency            string `json:"currency"`
	Status              string `json:"status"`
}

// MailOrderLifecycleData contains the structured payload for lifecycle email templates.
type MailOrderLifecycleData struct {
	RecipientEmail      string `json:"recipient_email"`
	RecipientName       string `json:"recipient_name,omitempty"`
	OrderID             string `json:"order_id"`
	OrderStatus         string `json:"order_status"`
	EventKind           string `json:"event_kind"`
	GigTitle            string `json:"gig_title"`
	PackageTier         string `json:"package_tier"`
	PackageDescription  string `json:"package_description"`
	PackageDeliveryDays int32  `json:"package_delivery_days"`
	RevisionCount       int32  `json:"revision_count"`
	PriceCents          int64  `json:"price_cents"`
	PriceDisplay        string `json:"price_display"`
	Currency            string `json:"currency"`
	Message             string `json:"message"`
	ActionLabel         string `json:"action_label,omitempty"`
	ActionURL           string `json:"action_url,omitempty"`
}

// realtimeDeliveryMessage mirrors the envelope consumed by realtime-service.
type realtimeDeliveryMessage struct {
	ConnectionID  string          `json:"connection_id"`
	UserID        string          `json:"user_id"`
	DeliveryScope string          `json:"delivery_scope,omitempty"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
}

// DeliverOrderCommand stores the seller delivery transition input.
type DeliverOrderCommand struct {
	OrderID       string
	SellerID      string
	Message       string
	AttachmentIDs []string
	RequestedAt   string
}

// DeliverOrderResult reports the seller delivery transition outcome.
type DeliverOrderResult struct {
	OrderID     string
	Status      string
	CurrentStep string
}

// AcceptDeliveryCommand stores the buyer acceptance transition input.
type AcceptDeliveryCommand struct {
	OrderID     string
	BuyerID     string
	RequestedAt string
}

// AcceptDeliveryResult reports the buyer acceptance transition outcome.
type AcceptDeliveryResult struct {
	OrderID     string
	Status      string
	CurrentStep string
}

// RequestRevisionCommand stores the buyer revision request input.
type RequestRevisionCommand struct {
	OrderID     string
	BuyerID     string
	Reason      string
	RequestedAt string
}

// RequestRevisionResult reports the revision transition outcome.
type RequestRevisionResult struct {
	OrderID     string
	Status      string
	CurrentStep string
}

// OpenDisputeCommand stores the buyer dispute input.
type OpenDisputeCommand struct {
	OrderID     string
	BuyerID     string
	Reason      string
	RequestedAt string
}

// OpenDisputeResult reports the dispute transition outcome.
type OpenDisputeResult struct {
	OrderID     string
	Status      string
	CurrentStep string
}

// SaveDeliveryCommand persists the delivery snapshot in order-service.
type SaveDeliveryCommand struct {
	OrderID       string
	SellerID      string
	Message       string
	AttachmentIDs []string
	RequestedAt   string
}

// SaveDeliveryResult reports the delivery status.
type SaveDeliveryResult struct {
	OrderID string
	Status  string
}

// MarkReleasePendingCommand marks an order as awaiting payout release.
type MarkReleasePendingCommand struct {
	OrderID          string
	PaymentReleaseID string
	RequestedAt      string
}

// MarkReleasePendingResult reports the release-pending order state.
type MarkReleasePendingResult struct {
	OrderID string
	Status  string
}

// MarkOrderCompletedCommand marks an order completed after release succeeds.
type MarkOrderCompletedCommand struct {
	OrderID          string
	PaymentReleaseID string
	RequestedAt      string
}

// MarkOrderCompletedResult reports the completed order state.
type MarkOrderCompletedResult struct {
	OrderID string
	Status  string
}

// MarkReleaseFailedCommand marks payout release as failed.
type MarkReleaseFailedCommand struct {
	OrderID     string
	Reason      string
	RequestedAt string
}

// MarkReleaseFailedResult reports the failed release state.
type MarkReleaseFailedResult struct {
	OrderID string
	Status  string
}

// releaseFundsRequestMessage enqueues the async payout-release workflow.
type releaseFundsRequestMessage struct {
	SagaID      string `json:"saga_id"`
	OrderID     string `json:"order_id"`
	RequestedAt string `json:"requested_at"`
}

// Config aliases the NATS configuration used by the service.
type Config = config.NATSConfig

// SessionRepository aliases the domain persistence contract.
type SessionRepository = domain.SessionRepository

// StepRepository aliases the domain persistence contract.
type StepRepository = domain.StepRepository
