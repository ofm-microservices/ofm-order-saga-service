package scylla

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/internal/domain"
)

type sessionRepository struct {
	db  *gocql.Session
	log logging.Logger
}

type stepRepository struct {
	db  *gocql.Session
	log logging.Logger
}

// NewSessionRepository constructs the Scylla-backed order saga session repository.
func NewSessionRepository(db *gocql.Session, log logging.Logger) (domain.SessionRepository, error) {
	if db == nil {
		return nil, errors.New("scylla session is nil")
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &sessionRepository{db: db, log: log.With(logging.String("module", "scylla-session-repository"))}, nil
}

// NewStepRepository constructs the Scylla-backed order saga step repository.
func NewStepRepository(db *gocql.Session, log logging.Logger) (domain.StepRepository, error) {
	if db == nil {
		return nil, errors.New("scylla session is nil")
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &stepRepository{db: db, log: log.With(logging.String("module", "scylla-step-repository"))}, nil
}

func (r *sessionRepository) Create(ctx context.Context, session domain.Session) (*domain.Session, error) {
	now := time.Now().UTC()
	if err := r.db.Query(insertSessionQuery, session.SagaID, session.OrderID, session.BuyerID, session.SellerID, session.SellerUsername, session.BuyerEmail, session.RealtimeConnectionID, session.GigID, session.GigTitle, session.PackageID, session.PackageTier, session.PackageDescription, session.PackageDeliveryDays, session.PriceCents, session.Currency, session.Status, now, now).WithContext(ctx).Exec(); err != nil {
		r.log.Error("create order saga session failed", logging.Operation("db.order_saga.session.create"), logging.String("saga_id", session.SagaID), logging.Err(err))
		return nil, err
	}
	session.CreatedAt = now
	session.UpdatedAt = now
	return &session, nil
}

func (r *sessionRepository) GetByID(ctx context.Context, sagaID string) (*domain.Session, error) {
	var row SessionRow
	if err := r.db.Query(getSessionByIDQuery, sagaID).WithContext(ctx).Consistency(gocql.One).Scan(&row.SagaID, &row.OrderID, &row.BuyerID, &row.SellerID, &row.SellerUsername, &row.BuyerEmail, &row.RealtimeConnectionID, &row.GigID, &row.GigTitle, &row.PackageID, &row.PackageTier, &row.PackageDescription, &row.PackageDeliveryDays, &row.PriceCents, &row.Currency, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrSessionNotFound
		}
		return nil, err
	}
	return &domain.Session{SagaID: row.SagaID, OrderID: row.OrderID, BuyerID: row.BuyerID, SellerID: row.SellerID, SellerUsername: row.SellerUsername, BuyerEmail: row.BuyerEmail, RealtimeConnectionID: row.RealtimeConnectionID, GigID: row.GigID, GigTitle: row.GigTitle, PackageID: row.PackageID, PackageTier: row.PackageTier, PackageDescription: row.PackageDescription, PackageDeliveryDays: row.PackageDeliveryDays, PriceCents: row.PriceCents, Currency: row.Currency, Status: row.Status, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}, nil
}

func (r *sessionRepository) GetByOrderID(ctx context.Context, orderID string) (*domain.Session, error) {
	var row SessionRow
	if err := r.db.Query(getSessionByOrderIDQuery, orderID).WithContext(ctx).Consistency(gocql.One).Scan(&row.SagaID, &row.OrderID, &row.BuyerID, &row.SellerID, &row.SellerUsername, &row.BuyerEmail, &row.RealtimeConnectionID, &row.GigID, &row.GigTitle, &row.PackageID, &row.PackageTier, &row.PackageDescription, &row.PackageDeliveryDays, &row.PriceCents, &row.Currency, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrSessionNotFound
		}
		return nil, err
	}
	return &domain.Session{SagaID: row.SagaID, OrderID: row.OrderID, BuyerID: row.BuyerID, SellerID: row.SellerID, SellerUsername: row.SellerUsername, BuyerEmail: row.BuyerEmail, RealtimeConnectionID: row.RealtimeConnectionID, GigID: row.GigID, GigTitle: row.GigTitle, PackageID: row.PackageID, PackageTier: row.PackageTier, PackageDescription: row.PackageDescription, PackageDeliveryDays: row.PackageDeliveryDays, PriceCents: row.PriceCents, Currency: row.Currency, Status: row.Status, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}, nil
}

func (r *sessionRepository) UpdateStatus(ctx context.Context, sagaID, status string) error {
	now := time.Now().UTC()
	return r.db.Query(updateSessionStatusQuery, status, now, sagaID).WithContext(ctx).Exec()
}

func (r *stepRepository) Create(ctx context.Context, step domain.Step) (*domain.Step, error) {
	now := time.Now().UTC()
	if step.MaxAttempts == 0 {
		step.MaxAttempts = 6
	}
	if err := r.db.Query(insertStepQuery, step.SagaID, step.StepKey, step.Status, step.Attempt, step.MaxAttempts, step.NextAttemptAt, step.LockedUntil, step.LastError, step.IdempotencyKey, now, now).WithContext(ctx).Exec(); err != nil {
		r.log.Error("create order saga step failed", logging.Operation("db.order_saga.step.create"), logging.String("saga_id", step.SagaID), logging.String("step_key", step.StepKey), logging.Err(err))
		return nil, err
	}
	step.CreatedAt = now
	step.UpdatedAt = now
	return &step, nil
}

func (r *stepRepository) GetByKey(ctx context.Context, sagaID, stepKey string) (*domain.Step, error) {
	var row StepRow
	if err := r.db.Query(getStepByKeyQuery, sagaID, stepKey).WithContext(ctx).Consistency(gocql.One).Scan(&row.SagaID, &row.StepKey, &row.Status, &row.Attempt, &row.MaxAttempts, &row.NextAttemptAt, &row.LockedUntil, &row.LastError, &row.IdempotencyKey, &row.CreatedAt, &row.UpdatedAt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrStepNotFound
		}
		return nil, err
	}
	return &domain.Step{SagaID: row.SagaID, StepKey: row.StepKey, Status: row.Status, Attempt: row.Attempt, MaxAttempts: row.MaxAttempts, NextAttemptAt: row.NextAttemptAt, LockedUntil: row.LockedUntil, LastError: row.LastError, IdempotencyKey: row.IdempotencyKey, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}, nil
}

func (r *stepRepository) ListBySagaID(ctx context.Context, sagaID string) ([]domain.Step, error) {
	iter := r.db.Query(listStepsBySagaIDQuery, sagaID).WithContext(ctx).Consistency(gocql.One).Iter()
	defer iter.Close()
	var rows []domain.Step
	var row StepRow
	for iter.Scan(&row.SagaID, &row.StepKey, &row.Status, &row.Attempt, &row.MaxAttempts, &row.NextAttemptAt, &row.LockedUntil, &row.LastError, &row.IdempotencyKey, &row.CreatedAt, &row.UpdatedAt) {
		rows = append(rows, domain.Step{SagaID: row.SagaID, StepKey: row.StepKey, Status: row.Status, Attempt: row.Attempt, MaxAttempts: row.MaxAttempts, NextAttemptAt: row.NextAttemptAt, LockedUntil: row.LockedUntil, LastError: row.LastError, IdempotencyKey: row.IdempotencyKey, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt})
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *stepRepository) UpdateStatus(ctx context.Context, sagaID, stepKey, status string) error {
	now := time.Now().UTC()
	return r.db.Query(updateStepStatusQuery, status, now, sagaID, stepKey).WithContext(ctx).Exec()
}
