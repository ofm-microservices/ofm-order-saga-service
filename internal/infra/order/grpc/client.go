package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	orderwritev1 "github.com/ofm-microservices/ofm-common/proto/orderwrite/v1"
	grpcpkg "google.golang.org/grpc"
	app "order-saga-service/internal/application"
	sharedinfra "order-saga-service/internal/infra"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   orderwritev1.OrderWriteServiceClient
	log  logging.Logger
}

func New(cfg Config, log Logger) (Client, error) {
	if cfg.Address == "" {
		return nil, ErrEmptyAddress
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	conn, lg, err := sharedinfra.NewConn(cfg.Address, log, "order-write-client")
	if err != nil {
		return nil, err
	}
	return &client{conn: conn, cl: orderwritev1.NewOrderWriteServiceClient(conn), log: lg}, nil
}

func (c *client) CreateDraftOrder(ctx context.Context, cmd app.CreateDraftOrderCommand) (*app.CreateDraftOrderResult, error) {
	questions := make([]*orderwritev1.OrderQuestionSnapshot, 0, len(cmd.Questions))
	for _, q := range cmd.Questions {
		questions = append(questions, &orderwritev1.OrderQuestionSnapshot{
			QuestionId: q.QuestionID,
			Text:       q.Text,
			Type:       q.Type,
			Required:   q.Required,
			SortOrder:  q.SortOrder,
			Options:    nil,
		})
	}
	res, err := c.cl.CreateDraftOrder(ctx, &orderwritev1.CreateDraftOrderRequest{
		SagaId:                     cmd.SagaID,
		OrderId:                    cmd.OrderID,
		BuyerUserId:                cmd.BuyerID,
		SellerUserId:               cmd.SellerID,
		SellerUsername:             cmd.SellerUsername,
		GigId:                      cmd.GigID,
		GigTitleSnapshot:           cmd.GigTitle,
		PackageId:                  cmd.PackageID,
		PackageTitleSnapshot:       cmd.PackageTier,
		PackageDescriptionSnapshot: cmd.PackageDescription,
		PriceAmountSnapshot:        cmd.PriceCents,
		PriceCurrencySnapshot:      cmd.Currency,
		DeliveryDaysSnapshot:       cmd.PackageDeliveryDays,
		RevisionCountSnapshot:      0,
		Questions:                  questions,
		IdempotencyKey:             cmd.IdempotencyKey,
		RequestedAt:                cmd.RequestedAt,
	})
	if err != nil {
		return nil, err
	}
	if res.GetOrder() == nil {
		return &app.CreateDraftOrderResult{OrderID: cmd.OrderID, Status: "requirements_pending"}, nil
	}
	return &app.CreateDraftOrderResult{OrderID: cmd.OrderID, Status: res.GetOrder().GetStatus()}, nil
}

func (c *client) SaveRequirementAnswers(ctx context.Context, cmd app.SaveRequirementAnswersCommand) (*app.SaveRequirementAnswersResult, error) {
	answers := make([]*orderwritev1.OrderAnswer, 0, len(cmd.Answers))
	for _, a := range cmd.Answers {
		answers = append(answers, &orderwritev1.OrderAnswer{QuestionId: a.QuestionID, Value: a.Value})
	}
	_, err := c.cl.SaveRequirementAnswers(ctx, &orderwritev1.SaveRequirementAnswersRequest{
		OrderId: cmd.OrderID,
		Answers: answers,
	})
	if err != nil {
		return nil, err
	}
	return &app.SaveRequirementAnswersResult{OrderID: cmd.OrderID, Status: "requirements_completed"}, nil
}

func (c *client) SaveBuyerInitialMessage(ctx context.Context, cmd app.SaveBuyerInitialMessageCommand) (*app.SaveBuyerInitialMessageResult, error) {
	_, err := c.cl.SaveBuyerInitialMessage(ctx, &orderwritev1.SaveBuyerInitialMessageRequest{
		OrderId: cmd.OrderID,
		Message: cmd.Message,
	})
	if err != nil {
		return nil, err
	}
	return &app.SaveBuyerInitialMessageResult{OrderID: cmd.OrderID, Status: "message_completed"}, nil
}

func (c *client) AttachFile(ctx context.Context, cmd app.AttachFileCommand) (*app.AttachFileResult, error) {
	_, err := c.cl.AttachFileToOrder(ctx, &orderwritev1.AttachFileToOrderRequest{
		OrderId:      cmd.OrderID,
		AttachmentId: cmd.AttachmentID,
	})
	if err != nil {
		return nil, err
	}
	return &app.AttachFileResult{OrderID: cmd.OrderID, Status: "attachments_completed"}, nil
}

func (c *client) MarkPaymentPending(ctx context.Context, cmd app.MarkPaymentPendingCommand) (*app.MarkPaymentPendingResult, error) {
	_, err := c.cl.MarkPaymentPending(ctx, &orderwritev1.MarkPaymentPendingRequest{OrderId: cmd.OrderID, PaymentId: cmd.PaymentIntentID, CheckoutUrl: cmd.CheckoutURL, RequestedAt: cmd.RequestedAt})
	if err != nil {
		return nil, err
	}
	return &app.MarkPaymentPendingResult{OrderID: cmd.OrderID, Status: "payment_pending"}, nil
}

func (c *client) MarkOrderFunded(ctx context.Context, cmd app.MarkOrderFundedCommand) (*app.MarkOrderFundedResult, error) {
	_, err := c.cl.MarkOrderFunded(ctx, &orderwritev1.MarkOrderFundedRequest{OrderId: cmd.OrderID, PaymentId: cmd.PaymentIntentID, RequestedAt: cmd.RequestedAt})
	if err != nil {
		return nil, err
	}
	return &app.MarkOrderFundedResult{OrderID: cmd.OrderID, Status: "funded"}, nil
}

func (c *client) MarkPaymentFailed(ctx context.Context, cmd app.MarkPaymentFailedCommand) (*app.MarkPaymentFailedResult, error) {
	_, err := c.cl.MarkPaymentFailed(ctx, &orderwritev1.MarkPaymentFailedRequest{OrderId: cmd.OrderID, PaymentId: "", Reason: cmd.Reason})
	if err != nil {
		return nil, err
	}
	return &app.MarkPaymentFailedResult{OrderID: cmd.OrderID, Status: "failed"}, nil
}

func (c *client) GetOrderLifecycleSnapshot(ctx context.Context, orderID string) (*app.OrderLifecycleSnapshot, error) {
	res, err := c.cl.GetOrderLifecycleSnapshot(ctx, &orderwritev1.GetOrderLifecycleSnapshotRequest{OrderId: orderID})
	if err != nil {
		return nil, err
	}
	o := res.GetOrder()
	if o == nil {
		return nil, nil
	}
	return &app.OrderLifecycleSnapshot{
		OrderID:               o.GetOrderId(),
		SagaID:                o.GetSagaId(),
		BuyerID:               o.GetBuyerUserId(),
		SellerID:              o.GetSellerUserId(),
		SellerUsername:        o.GetSellerUsername(),
		GigID:                 o.GetGigId(),
		GigTitle:              o.GetGigTitleSnapshot(),
		PackageID:             o.GetPackageId(),
		PackageTitle:          o.GetPackageTitleSnapshot(),
		PackageDescription:    o.GetPackageDescriptionSnapshot(),
		PriceCents:            o.GetPriceAmountSnapshot(),
		Currency:              o.GetPriceCurrencySnapshot(),
		Status:                o.GetStatus(),
		RevisionCountSnapshot: o.GetRevisionCountSnapshot(),
		RevisionCountUsed:     o.GetRevisionCountUsed(),
		BuyerResponseDeadline: o.GetBuyerResponseDeadline(),
		PaymentIntentID:       o.GetPaymentIntentId(),
		PaymentReleaseID:      o.GetPaymentReleaseId(),
		DeliveredAt:           o.GetDeliveredAt(),
		CompletedAt:           o.GetCompletedAt(),
		DisputedAt:            o.GetDisputedAt(),
	}, nil
}

func (c *client) SaveDelivery(ctx context.Context, cmd app.SaveDeliveryCommand) (*app.SaveDeliveryResult, error) {
	_, err := c.cl.SaveDelivery(ctx, &orderwritev1.SaveDeliveryRequest{OrderId: cmd.OrderID, SellerUserId: cmd.SellerID, DeliveryMessage: cmd.Message, AttachmentIds: cmd.AttachmentIDs, RequestedAt: cmd.RequestedAt})
	if err != nil {
		return nil, err
	}
	return &app.SaveDeliveryResult{OrderID: cmd.OrderID, Status: "delivered"}, nil
}

func (c *client) MarkReleasePending(ctx context.Context, cmd app.MarkReleasePendingCommand) (*app.MarkReleasePendingResult, error) {
	_, err := c.cl.MarkReleasePending(ctx, &orderwritev1.MarkReleasePendingRequest{OrderId: cmd.OrderID, PaymentReleaseId: cmd.PaymentReleaseID, RequestedAt: cmd.RequestedAt})
	if err != nil {
		return nil, err
	}
	return &app.MarkReleasePendingResult{OrderID: cmd.OrderID, Status: "release_pending"}, nil
}

func (c *client) RequestRevision(ctx context.Context, cmd app.RequestRevisionCommand) (*app.RequestRevisionResult, error) {
	_, err := c.cl.RequestRevision(ctx, &orderwritev1.RequestRevisionRequest{OrderId: cmd.OrderID, BuyerUserId: cmd.BuyerID, Reason: cmd.Reason, RequestedAt: cmd.RequestedAt})
	if err != nil {
		return nil, err
	}
	return &app.RequestRevisionResult{OrderID: cmd.OrderID, Status: "revision_requested"}, nil
}

func (c *client) OpenDispute(ctx context.Context, cmd app.OpenDisputeCommand) (*app.OpenDisputeResult, error) {
	_, err := c.cl.OpenDispute(ctx, &orderwritev1.OpenDisputeRequest{OrderId: cmd.OrderID, BuyerUserId: cmd.BuyerID, Reason: cmd.Reason, RequestedAt: cmd.RequestedAt})
	if err != nil {
		return nil, err
	}
	return &app.OpenDisputeResult{OrderID: cmd.OrderID, Status: "disputed"}, nil
}

func (c *client) MarkOrderCompleted(ctx context.Context, cmd app.MarkOrderCompletedCommand) (*app.MarkOrderCompletedResult, error) {
	_, err := c.cl.MarkOrderCompleted(ctx, &orderwritev1.MarkOrderCompletedRequest{OrderId: cmd.OrderID, PaymentReleaseId: cmd.PaymentReleaseID, RequestedAt: cmd.RequestedAt})
	if err != nil {
		return nil, err
	}
	return &app.MarkOrderCompletedResult{OrderID: cmd.OrderID, Status: "completed"}, nil
}

func (c *client) MarkReleaseFailed(ctx context.Context, cmd app.MarkReleaseFailedCommand) (*app.MarkReleaseFailedResult, error) {
	_, err := c.cl.MarkReleaseFailed(ctx, &orderwritev1.MarkReleaseFailedRequest{OrderId: cmd.OrderID, Reason: cmd.Reason, RequestedAt: cmd.RequestedAt})
	if err != nil {
		return nil, err
	}
	return &app.MarkReleaseFailedResult{OrderID: cmd.OrderID, Status: "release_failed"}, nil
}

func (c *client) GetOrderPaymentSnapshot(ctx context.Context, orderID string) (*app.OrderPaymentSnapshot, error) {
	res, err := c.cl.GetOrderPaymentSnapshot(ctx, &orderwritev1.GetOrderPaymentSnapshotRequest{OrderId: orderID})
	if err != nil {
		return nil, err
	}
	o := res.GetOrder()
	if o == nil {
		return nil, nil
	}
	return &app.OrderPaymentSnapshot{OrderID: o.GetOrderId(), SagaID: o.GetSagaId(), BuyerID: o.GetBuyerUserId(), SellerID: o.GetSellerUserId(), SellerUsername: o.GetSellerUsername(), GigTitle: o.GetGigTitleSnapshot(), PackageTitle: o.GetPackageTitleSnapshot(), PriceCents: o.GetPriceAmountSnapshot(), Currency: o.GetPriceCurrencySnapshot(), Status: o.GetStatus()}, nil
}

func (c *client) Close() error {
	if c == nil {
		return nil
	}
	return sharedinfra.CloseConn(c.conn)
}
