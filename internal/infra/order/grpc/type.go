package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	app "order-saga-service/internal/application"
)

// Logger aliases the shared logger.
type Logger = logging.Logger

// Client is the order-service gRPC boundary used by the saga.
type Client interface {
	CreateDraftOrder(ctx context.Context, cmd app.CreateDraftOrderCommand) (*app.CreateDraftOrderResult, error)
	SaveRequirementAnswers(ctx context.Context, cmd app.SaveRequirementAnswersCommand) (*app.SaveRequirementAnswersResult, error)
	SaveBuyerInitialMessage(ctx context.Context, cmd app.SaveBuyerInitialMessageCommand) (*app.SaveBuyerInitialMessageResult, error)
	AttachFile(ctx context.Context, cmd app.AttachFileCommand) (*app.AttachFileResult, error)
	MarkPaymentPending(ctx context.Context, cmd app.MarkPaymentPendingCommand) (*app.MarkPaymentPendingResult, error)
	MarkOrderFunded(ctx context.Context, cmd app.MarkOrderFundedCommand) (*app.MarkOrderFundedResult, error)
	MarkPaymentFailed(ctx context.Context, cmd app.MarkPaymentFailedCommand) (*app.MarkPaymentFailedResult, error)
	GetOrderPaymentSnapshot(ctx context.Context, orderID string) (*app.OrderPaymentSnapshot, error)
	GetOrderLifecycleSnapshot(ctx context.Context, orderID string) (*app.OrderLifecycleSnapshot, error)
	SaveDelivery(ctx context.Context, cmd app.SaveDeliveryCommand) (*app.SaveDeliveryResult, error)
	MarkReleasePending(ctx context.Context, cmd app.MarkReleasePendingCommand) (*app.MarkReleasePendingResult, error)
	RequestRevision(ctx context.Context, cmd app.RequestRevisionCommand) (*app.RequestRevisionResult, error)
	OpenDispute(ctx context.Context, cmd app.OpenDisputeCommand) (*app.OpenDisputeResult, error)
	MarkOrderCompleted(ctx context.Context, cmd app.MarkOrderCompletedCommand) (*app.MarkOrderCompletedResult, error)
	MarkDisputeResolved(ctx context.Context, cmd app.MarkDisputeResolvedCommand) (*app.MarkDisputeResolvedResult, error)
	MarkReleaseFailed(ctx context.Context, cmd app.MarkReleaseFailedCommand) (*app.MarkReleaseFailedResult, error)
	Close() error
}

// Config defines the order-service target.
type Config struct {
	Address string `env:"ADDRESS,required"`
}
