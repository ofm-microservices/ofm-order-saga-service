package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	application "order-saga-service/internal/application"

	"go.uber.org/fx"
)

// ServiceModule provides the application service.
var ServiceModule = fx.Options(
	fx.Provide(ProvideService),
)

// ProvideService constructs the saga application service.
func ProvideService(sessions application.SessionRepository, steps application.StepRepository, gigs application.GigSnapshotClient, auth application.AuthQueryClient, orders application.OrderWriteClient, payments application.PaymentCheckoutClient, broker application.EventBroker, cfg *config.Config, lg logging.Logger) (application.Service, error) {
	return application.New(sessions, steps, gigs, auth, orders, payments, broker, cfg.NATS, lg)
}
