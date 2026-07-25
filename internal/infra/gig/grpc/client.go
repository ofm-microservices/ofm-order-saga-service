package grpc

import (
	"context"
	"strings"

	app "order-saga-service/internal/application"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	gigv1 "github.com/ofm-microservices/ofm-common/proto/gig/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   gigv1.GigCommandServiceClient
	log  logging.Logger
}

// New constructs the gig-service gRPC client used by the saga.
func New(cfg Config, log Logger) (Client, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, ErrEmptyGigServiceAddress
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	conn, err := grpcpkg.NewClient(
		cfg.Address,
		grpcpkg.WithTransportCredentials(insecure.NewCredentials()),
		grpcpkg.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, err
	}

	return &client{
		conn: conn,
		cl:   gigv1.NewGigCommandServiceClient(conn),
		log:  log.With(logging.String("module", "gig-service-client"), logging.String("address", cfg.Address)),
	}, nil
}

func (c *client) GetOrderStartSnapshot(ctx context.Context, gigID, packageID string) (*app.OrderStartSnapshot, error) {
	res, err := c.cl.GetOrderStartSnapshot(ctx, &gigv1.GetOrderStartSnapshotRequest{
		GigId:     gigID,
		PackageId: packageID,
	})
	if err != nil {
		return nil, err
	}
	snapshot := res.GetSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	out := &app.OrderStartSnapshot{
		GigID:              snapshot.GetGigId(),
		PackageID:          snapshot.GetPackageId(),
		SellerID:           snapshot.GetSellerUserId(),
		SellerUsername:     snapshot.GetSellerUsername(),
		GigTitle:           snapshot.GetGigTitle(),
		PictureFileID:      snapshot.GetPictureFileId(),
		PackageTitle:       snapshot.GetPackageTitle(),
		PackageDescription: snapshot.GetPackageDescription(),
		PriceCents:         snapshot.GetPriceCents(),
		Currency:           snapshot.GetCurrency(),
		DeliveryDays:       snapshot.GetDeliveryDays(),
		RevisionCount:      snapshot.GetRevisionCount(),
		GigPublished:       snapshot.GetGigPublished(),
		PackageAvailable:   snapshot.GetPackageAvailable(),
	}
	if len(snapshot.GetQuestions()) > 0 {
		out.Questions = make([]app.OrderStartQuestion, 0, len(snapshot.GetQuestions()))
		for _, q := range snapshot.GetQuestions() {
			out.Questions = append(out.Questions, app.OrderStartQuestion{
				ID:        q.GetId(),
				Text:      q.GetContent(),
				SortOrder: q.GetSortOrder(),
			})
		}
	}
	return out, nil
}

func (c *client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.log.Info("closing gig service grpc client")
	return c.conn.Close()
}
