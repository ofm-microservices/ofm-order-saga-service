package grpc

import (
	"context"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	orderwritev1 "github.com/ofm-microservices/ofm-common/proto/orderwrite/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	app "order-saga-service/internal/application"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   orderwritev1.OrderWriteServiceClient
	log  logging.Logger
}

func New(cfg Config, log Logger) (Client, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, ErrEmptyAddress
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	conn, err := grpcpkg.NewClient(cfg.Address, grpcpkg.WithTransportCredentials(insecure.NewCredentials()), grpcpkg.WithStatsHandler(otelgrpc.NewClientHandler()), grpcpkg.WithUnaryInterceptor(metrics.UnaryClientInterceptor()))
	if err != nil {
		return nil, err
	}
	return &client{conn: conn, cl: orderwritev1.NewOrderWriteServiceClient(conn), log: log.With(logging.String("module", "order-write-client"), logging.String("address", cfg.Address))}, nil
}

func (c *client) CreateDraftOrder(ctx context.Context, cmd app.CreateDraftOrderCommand) (*app.CreateDraftOrderResult, error) {
	questions := make([]*orderwritev1.OrderQuestionSnapshot, 0, len(cmd.Questions))
	for _, q := range cmd.Questions {
		questions = append(questions, &orderwritev1.OrderQuestionSnapshot{
			QuestionId:  q.QuestionID,
			Text:        q.Text,
			Type:        q.Type,
			Required:    q.Required,
			SortOrder:   q.SortOrder,
			Options:     nil,
		})
	}
	res, err := c.cl.CreateDraftOrder(ctx, &orderwritev1.CreateDraftOrderRequest{
		SagaId: cmd.SagaID, OrderId: cmd.OrderID, BuyerUserId: cmd.BuyerID, SellerUserId: cmd.SellerID, GigId: cmd.GigID, GigTitleSnapshot: cmd.GigTitle, PackageId: cmd.PackageID, PackageTitleSnapshot: cmd.PackageTier, PackageDescriptionSnapshot: cmd.PackageDescription, PriceAmountSnapshot: cmd.PriceCents, PriceCurrencySnapshot: cmd.Currency, DeliveryDaysSnapshot: cmd.PackageDeliveryDays, RevisionCountSnapshot: 0, Questions: questions, IdempotencyKey: cmd.IdempotencyKey, RequestedAt: cmd.RequestedAt,
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

func (c *client) GetOrderPaymentSnapshot(ctx context.Context, orderID string) (*app.OrderPaymentSnapshot, error) {
	res, err := c.cl.GetOrderPaymentSnapshot(ctx, &orderwritev1.GetOrderPaymentSnapshotRequest{OrderId: orderID})
	if err != nil {
		return nil, err
	}
	o := res.GetOrder()
	if o == nil {
		return nil, nil
	}
	return &app.OrderPaymentSnapshot{OrderID: o.GetOrderId(), SagaID: o.GetSagaId(), BuyerID: o.GetBuyerUserId(), SellerID: o.GetSellerUserId(), GigTitle: o.GetGigTitleSnapshot(), PackageTitle: o.GetPackageTitleSnapshot(), PriceCents: o.GetPriceAmountSnapshot(), Currency: o.GetPriceCurrencySnapshot(), Status: o.GetStatus()}, nil
}

func (c *client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
