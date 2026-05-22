package grpc

import (
	"context"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	paymentcheckoutv1 "github.com/ofm-microservices/ofm-common/proto/paymentcheckout/v1"
	paymentconnectv1 "github.com/ofm-microservices/ofm-common/proto/paymentconnect/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	app "order-saga-service/internal/application"
)

type client struct {
	conn     *grpcpkg.ClientConn
	checkout paymentcheckoutv1.PaymentCheckoutServiceClient
	connect  paymentconnectv1.PaymentOnboardingServiceClient
	log      logging.Logger
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
	return &client{conn: conn, checkout: paymentcheckoutv1.NewPaymentCheckoutServiceClient(conn), connect: paymentconnectv1.NewPaymentOnboardingServiceClient(conn), log: log.With(logging.String("module", "payment-checkout-client"), logging.String("address", cfg.Address))}, nil
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
	return &app.CreateCheckoutSessionResult{PaymentIntentID: res.GetStripePaymentIntentId(), CheckoutURL: res.GetCheckoutUrl(), Status: res.GetStatus()}, nil
}

func (c *client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
