package domain

import (
	"context"
	"time"
)

const (
	// SessionStatusStarted marks a newly created order saga.
	SessionStatusStarted = "started"
	// SessionStatusRequirementsPending marks the order as awaiting buyer requirements.
	SessionStatusRequirementsPending = "requirements_pending"
	// SessionStatusPendingOrder marks the order creation step in progress.
	SessionStatusPendingOrder = "pending_order"
	// SessionStatusPendingPayment marks the payment intent step in progress.
	SessionStatusPendingPayment = "pending_payment"
	// SessionStatusPaymentConfirmed marks the saga after payment confirmation.
	SessionStatusPaymentConfirmed = "payment_confirmed"
	// SessionStatusCompleted marks a finalized order saga.
	SessionStatusCompleted = "completed"
	// SessionStatusFailed marks a failed order saga.
	SessionStatusFailed = "failed"

	// StepStatusPending marks a step created but not yet dispatched.
	StepStatusPending = "pending"
	// StepStatusInProgress marks a step waiting on a downstream result.
	StepStatusInProgress = "in_progress"
	// StepStatusCompleted marks a step that finished successfully.
	StepStatusCompleted = "completed"
	// StepStatusFailed marks a step that finished with a failure result.
	StepStatusFailed = "failed"

	// StepKeyCreateOrder identifies the order creation command.
	StepKeyCreateOrder = "order.create"
	// StepKeyCreatePaymentIntent identifies the payment intent command.
	StepKeyCreatePaymentIntent = "payment.intent"
	// StepKeyAwaitPaymentWebhook identifies the webhook-driven completion step.
	StepKeyAwaitPaymentWebhook = "await_payment_webhook"
	// StepKeySendReceipt identifies the receipt email step.
	StepKeySendReceipt = "mail.receipt"
	// StepKeyRealtimeOrderAccepted identifies the accepted notification step.
	StepKeyRealtimeOrderAccepted = "realtime.order.accepted"
	// StepKeyRealtimePaymentReady identifies the payment-ready notification step.
	StepKeyRealtimePaymentReady = "realtime.payment.ready"
	// StepKeyRealtimeOrderConfirmed identifies the confirmed notification step.
	StepKeyRealtimeOrderConfirmed = "realtime.order.confirmed"
	// StepKeyRealtimeOrderFailed identifies the failed notification step.
	StepKeyRealtimeOrderFailed = "realtime.order.failed"
)

// Session is the persisted write-model snapshot of an order saga.
type Session struct {
	SagaID               string
	OrderID              string
	BuyerID              string
	SellerID             string
	BuyerEmail           string
	RealtimeConnectionID string
	GigID                string
	GigTitle             string
	PackageID            string
	PackageTier          string
	PackageDescription   string
	PackageDeliveryDays  int32
	PriceCents           int64
	Currency             string
	Status               string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// Step is the persisted execution state of one orchestration step.
type Step struct {
	SagaID    string
	StepKey   string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SessionRepository persists order saga sessions owned by the saga domain.
type SessionRepository interface {
	Create(ctx context.Context, session Session) (*Session, error)
	GetByID(ctx context.Context, sagaID string) (*Session, error)
	GetByOrderID(ctx context.Context, orderID string) (*Session, error)
	UpdateStatus(ctx context.Context, sagaID, status string) error
}

// StepRepository persists order saga step state owned by the saga domain.
type StepRepository interface {
	Create(ctx context.Context, step Step) (*Step, error)
	GetByKey(ctx context.Context, sagaID, stepKey string) (*Step, error)
	ListBySagaID(ctx context.Context, sagaID string) ([]Step, error)
	UpdateStatus(ctx context.Context, sagaID, stepKey, status string) error
}
