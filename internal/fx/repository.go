package appfx

import (
	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/internal/domain"
	scyllarepo "order-saga-service/internal/infra/scylla"

	"go.uber.org/fx"
)

// RepoModule provides the concrete saga repositories.
var RepoModule = fx.Options(
	fx.Provide(ProvideSessionRepository),
	fx.Provide(ProvideStepRepository),
)

// ProvideSessionRepository constructs the Scylla-backed session repository.
func ProvideSessionRepository(db *gocql.Session, lg logging.Logger) (domain.SessionRepository, error) {
	return scyllarepo.NewSessionRepository(db, lg)
}

// ProvideStepRepository constructs the Scylla-backed step repository.
func ProvideStepRepository(db *gocql.Session, lg logging.Logger) (domain.StepRepository, error) {
	return scyllarepo.NewStepRepository(db, lg)
}
