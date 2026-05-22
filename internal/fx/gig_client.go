package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
	giggrpc "order-saga-service/internal/infra/gig/grpc"

	"go.uber.org/fx"
)

// GigClientModule wires the gig-service gRPC client dependency.
var GigClientModule = fx.Options(
	fx.Provide(ProvideGigSnapshotClient),
)

// ProvideGigSnapshotClient constructs the gig-service snapshot client.
func ProvideGigSnapshotClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.GigSnapshotClient, error) {
	client, err := giggrpc.New(giggrpc.Config{Address: cfg.Gig.Address}, lg)
	if err != nil {
		lg.Error("connect gig service grpc failed", logging.Err(err))
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		return client.Close()
	}})
	return client, nil
}
