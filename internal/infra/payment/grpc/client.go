package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	paymentcheckoutv1 "github.com/ofm-microservices/ofm-common/proto/paymentcheckout/v1"
	paymentconnectv1 "github.com/ofm-microservices/ofm-common/proto/paymentconnect/v1"
	grpcpkg "google.golang.org/grpc"
	app "order-saga-service/internal/application"
	sharedinfra "order-saga-service/internal/infra"
)

type client struct {
	conn     *grpcpkg.ClientConn
	checkout paymentcheckoutv1.PaymentCheckoutServiceClient
	connect  paymentconnectv1.PaymentOnboardingServiceClient
	log      logging.Logger
}

func New(cfg Config, log Logger) (Client, error) {
	if cfg.Address == "" {
		return nil, ErrEmptyAddress
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	conn, lg, err := sharedinfra.NewConn(cfg.Address, log, "payment-checkout-client")
	if err != nil {
		return nil, err
	}
	return &client{conn: conn, checkout: paymentcheckoutv1.NewPaymentCheckoutServiceClient(conn), connect: paymentconnectv1.NewPaymentOnboardingServiceClient(conn), log: lg}, nil
}

func (c *client) GetConnectStatus(ctx context.Context, userID string) (*app.GetConnectStatusResult, error) {
	res, err := c.connect.GetConnectStatus(ctx, &paymentconnectv1.GetConnectStatusRequest{UserId: userID})
	if err != nil {
		return nil, err
	}
	return &app.GetConnectStatusResult{UserID: res.GetUserId(), StripeAccountID: res.GetStripeAccountId(), Status: res.GetStatus(), DisabledReason: res.GetDisabledReason(), OccurredAt: res.GetOccurredAt()}, nil
}

func (c *client) CreateCheckoutSession(ctx context.Context, cmd app.CreateCheckoutSessionCommand) (*app.CreateCheckoutSessionResult, error) {
	res, err := c.checkout.CreateCheckoutSession(ctx, &paymentcheckoutv1.CreateCheckoutSessionRequest{
		SagaId:         cmd.SagaID,
		OrderId:        cmd.OrderID,
		BuyerUserId:    cmd.BuyerID,
		SellerUserId:   cmd.SellerID,
		AmountCents:    cmd.AmountCents,
		Currency:       cmd.Currency,
		Title:          cmd.Title,
		IdempotencyKey: cmd.IdempotencyKey,
		RequestedAt:    cmd.RequestedAt,
	})
	if err != nil {
		return nil, err
	}
	return &app.CreateCheckoutSessionResult{PaymentIntentID: res.GetPaymentId(), CheckoutURL: res.GetCheckoutUrl(), Status: res.GetStatus()}, nil
}

func (c *client) ReleaseFunds(ctx context.Context, cmd app.ReleaseFundsCommand) (*app.ReleaseFundsResult, error) {
	res, err := c.checkout.ReleaseFunds(ctx, &paymentcheckoutv1.ReleaseFundsRequest{
		OrderId:        cmd.OrderID,
		PaymentId:      cmd.PaymentID,
		SellerUserId:   cmd.SellerUserID,
		AmountCents:    cmd.AmountCents,
		Currency:       cmd.Currency,
		IdempotencyKey: cmd.IdempotencyKey,
		RequestedAt:    cmd.RequestedAt,
	})
	if err != nil {
		return nil, err
	}
	return &app.ReleaseFundsResult{
		OrderID:          res.GetOrderId(),
		PaymentReleaseID: res.GetPaymentReleaseId(),
		StripeTransferID: res.GetStripeTransferId(),
		Status:           res.GetStatus(),
		OccurredAt:       res.GetOccurredAt(),
	}, nil
}

func (c *client) GetReleaseByOrderID(ctx context.Context, orderID string) (*app.GetReleaseByOrderResult, error) {
	res, err := c.checkout.GetReleaseByOrderId(ctx, &paymentcheckoutv1.GetReleaseByOrderIdRequest{OrderId: orderID})
	if err != nil {
		return nil, err
	}
	return &app.GetReleaseByOrderResult{
		OrderID:          res.GetOrderId(),
		PaymentReleaseID: res.GetPaymentReleaseId(),
		PaymentID:        res.GetPaymentIntentId(),
		SellerUserID:     res.GetSellerUserId(),
		AmountCents:      res.GetAmountCents(),
		Currency:         res.GetCurrency(),
		IdempotencyKey:   res.GetIdempotencyKey(),
		StripeTransferID: res.GetStripeTransferId(),
		Status:           res.GetStatus(),
		FailureReason:    res.GetFailureReason(),
		OccurredAt:       res.GetOccurredAt(),
	}, nil
}

func (c *client) Close() error {
	if c == nil {
		return nil
	}
	return sharedinfra.CloseConn(c.conn)
}
