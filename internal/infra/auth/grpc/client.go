package grpc

import (
	"context"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	authv1 "github.com/ofm-microservices/ofm-common/proto/auth/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   authv1.AuthQueryServiceClient
	log  logging.Logger
}

// New constructs the auth-query gRPC client used by the saga.
func New(cfg Config, log logging.Logger) (Client, error) {
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
	return &client{conn: conn, cl: authv1.NewAuthQueryServiceClient(conn), log: log.With(logging.String("module", "auth-query-client"), logging.String("address", cfg.Address))}, nil
}

func (c *client) GetEmailByUserID(ctx context.Context, userID string) (string, error) {
	res, err := c.cl.GetEmailByUserID(ctx, &authv1.GetEmailByUserIDRequest{UserId: userID})
	if err != nil {
		return "", err
	}
	return res.GetEmail(), nil
}

func (c *client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
