package application

import "errors"

var (
	// ErrNilEventBroker reports a missing broker dependency.
	ErrNilEventBroker = errors.New("event broker is nil")
	// ErrNilSessionRepository reports a missing session repository dependency.
	ErrNilSessionRepository = errors.New("session repository is nil")
	// ErrNilStepRepository reports a missing step repository dependency.
	ErrNilStepRepository = errors.New("step repository is nil")
	// ErrNilGigSnapshotClient reports a missing gig snapshot dependency.
	ErrNilGigSnapshotClient = errors.New("gig snapshot client is nil")
	// ErrNilAuthQueryClient reports a missing auth query dependency.
	ErrNilAuthQueryClient = errors.New("auth query client is nil")
	// ErrNilOrderWriteClient reports a missing order write client dependency.
	ErrNilOrderWriteClient = errors.New("order write client is nil")
	// ErrNilPaymentCheckoutClient reports a missing payment checkout client dependency.
	ErrNilPaymentCheckoutClient = errors.New("payment checkout client is nil")
	// ErrNilLogger reports a missing logger dependency.
	ErrNilLogger = errors.New("logger is nil")
	// ErrPublishCommand reports a command publication failure.
	ErrPublishCommand = errors.New("publish command failed")
	// ErrInvalidOrderSnapshot reports an invalid gig snapshot response.
	ErrInvalidOrderSnapshot = errors.New("invalid order snapshot")
	// ErrSelfOrderNotAllowed reports that the buyer matches the gig owner.
	ErrSelfOrderNotAllowed = errors.New("self order not allowed")
	// ErrOrderNotOwned reports that the confirm request is not owned by the buyer.
	ErrOrderNotOwned = errors.New("order not owned")
	// ErrOrderNotConfirmable reports that the order cannot proceed to checkout yet.
	ErrOrderNotConfirmable = errors.New("order not confirmable")
	// ErrOrderAlreadyPaymentPending reports that checkout has already been requested.
	ErrOrderAlreadyPaymentPending = errors.New("order already payment pending")
	// ErrOrderAlreadyFunded reports that the order has already been funded.
	ErrOrderAlreadyFunded = errors.New("order already funded")
	// ErrConnectOnboardingIncomplete reports the seller cannot receive payment yet.
	ErrConnectOnboardingIncomplete = errors.New("connect onboarding incomplete")
	// ErrOrderNotDeliverable reports that the order cannot accept seller delivery yet.
	ErrOrderNotDeliverable = errors.New("order not deliverable")
	// ErrOrderNotAcceptable reports that the order cannot be accepted yet.
	ErrOrderNotAcceptable = errors.New("order not acceptable")
	// ErrOrderNotRevisionable reports that the order cannot accept a revision request.
	ErrOrderNotRevisionable = errors.New("order not revisionable")
	// ErrOrderNotDisputable reports that the order cannot be disputed yet.
	ErrOrderNotDisputable = errors.New("order not disputable")
	// ErrOrderReleaseFailed reports that the payout release failed.
	ErrOrderReleaseFailed = errors.New("order release failed")
)
