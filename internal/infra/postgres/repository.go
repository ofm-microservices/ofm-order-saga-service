package postgres

import (
	"context"
	"database/sql"
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/internal/domain"
	"time"
)

// NewSessionRepository constructs the PostgreSQL-backed saga session repository.
func NewSessionRepository(db *sqlx.DB, lg logging.Logger) (domain.SessionRepository, error) {
	if db == nil {
		return nil, errors.New("postgres database is nil")
	}
	if lg == nil {
		return nil, errors.New("logger is nil")
	}
	return &sessionRepository{db: db, log: lg}, nil
}

// NewStepRepository constructs the PostgreSQL-backed saga step repository.
func NewStepRepository(db *sqlx.DB, lg logging.Logger) (domain.StepRepository, error) {
	if db == nil {
		return nil, errors.New("postgres database is nil")
	}
	if lg == nil {
		return nil, errors.New("logger is nil")
	}
	return &stepRepository{db: db, log: lg}, nil
}

type sessionRepository struct {
	db  *sqlx.DB
	log logging.Logger
}
type stepRepository struct {
	db  *sqlx.DB
	log logging.Logger
}
type sessionRow struct {
	SagaID              string    `db:"saga_id"`
	OrderID             string    `db:"order_id"`
	BuyerID             string    `db:"buyer_id"`
	SellerID            string    `db:"seller_id"`
	SellerUsername      string    `db:"seller_username"`
	BuyerEmail          string    `db:"buyer_email"`
	GigID               string    `db:"gig_id"`
	GigTitle            string    `db:"gig_title"`
	PictureFileID       string    `db:"picture_file_id"`
	PackageID           string    `db:"package_id"`
	PackageTier         string    `db:"package_tier"`
	PackageDescription  string    `db:"package_description"`
	PackageDeliveryDays int32     `db:"package_delivery_days"`
	PriceCents          int64     `db:"price_cents"`
	Currency            string    `db:"currency"`
	Status              string    `db:"status"`
	CreatedAt           time.Time `db:"created_at"`
	UpdatedAt           time.Time `db:"updated_at"`
}
type stepRow struct {
	SagaID         string     `db:"saga_id"`
	StepKey        string     `db:"step_key"`
	Status         string     `db:"status"`
	Attempt        int32      `db:"attempt"`
	MaxAttempts    int32      `db:"max_attempts"`
	NextAttemptAt  *time.Time `db:"next_attempt_at"`
	LockedUntil    *time.Time `db:"locked_until"`
	LastError      string     `db:"last_error"`
	IdempotencyKey string     `db:"idempotency_key"`
	CreatedAt      time.Time  `db:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"`
}

func session(r sessionRow) *domain.Session {
	return &domain.Session{SagaID: r.SagaID, OrderID: r.OrderID, BuyerID: r.BuyerID, SellerID: r.SellerID, SellerUsername: r.SellerUsername, BuyerEmail: r.BuyerEmail, GigID: r.GigID, GigTitle: r.GigTitle, PictureFileID: r.PictureFileID, PackageID: r.PackageID, PackageTier: r.PackageTier, PackageDescription: r.PackageDescription, PackageDeliveryDays: r.PackageDeliveryDays, PriceCents: r.PriceCents, Currency: r.Currency, Status: r.Status, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}
}
func step(r stepRow) *domain.Step {
	return &domain.Step{SagaID: r.SagaID, StepKey: r.StepKey, Status: r.Status, Attempt: r.Attempt, MaxAttempts: r.MaxAttempts, NextAttemptAt: r.NextAttemptAt, LockedUntil: r.LockedUntil, LastError: r.LastError, IdempotencyKey: r.IdempotencyKey, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}
}

const sessionSelect = `SELECT saga_id,order_id,buyer_id,seller_id,seller_username,buyer_email,gig_id,gig_title,picture_file_id,package_id,package_tier,package_description,package_delivery_days,price_cents,currency,status,created_at,updated_at FROM order_saga_sessions `

func (r *sessionRepository) Create(ctx context.Context, s domain.Session) (*domain.Session, error) {
	now := time.Now().UTC()
	_, err := r.db.ExecContext(ctx, `INSERT INTO order_saga_sessions(saga_id,order_id,buyer_id,seller_id,seller_username,buyer_email,gig_id,gig_title,picture_file_id,package_id,package_tier,package_description,package_delivery_days,price_cents,currency,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, s.SagaID, s.OrderID, s.BuyerID, s.SellerID, s.SellerUsername, s.BuyerEmail, s.GigID, s.GigTitle, s.PictureFileID, s.PackageID, s.PackageTier, s.PackageDescription, s.PackageDeliveryDays, s.PriceCents, s.Currency, s.Status, now, now)
	if err != nil {
		return nil, err
	}
	s.CreatedAt = now
	s.UpdatedAt = now
	return &s, nil
}
func (r *sessionRepository) GetByID(ctx context.Context, id string) (*domain.Session, error) {
	var x sessionRow
	err := r.db.GetContext(ctx, &x, sessionSelect+`WHERE saga_id=$1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrSessionNotFound
	}
	if err != nil {
		return nil, err
	}
	return session(x), nil
}
func (r *sessionRepository) GetByOrderID(ctx context.Context, id string) (*domain.Session, error) {
	var x sessionRow
	err := r.db.GetContext(ctx, &x, sessionSelect+`WHERE order_id=$1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrSessionNotFound
	}
	if err != nil {
		return nil, err
	}
	return session(x), nil
}
func (r *sessionRepository) UpdateStatus(ctx context.Context, id, status string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE order_saga_sessions SET status=$1,updated_at=$2 WHERE saga_id=$3`, status, time.Now().UTC(), id)
	return err
}
func (r *stepRepository) Create(ctx context.Context, s domain.Step) (*domain.Step, error) {
	now := time.Now().UTC()
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 6
	}
	_, err := r.db.ExecContext(ctx, `INSERT INTO order_saga_steps(saga_id,step_key,status,attempt,max_attempts,next_attempt_at,locked_until,last_error,idempotency_key,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, s.SagaID, s.StepKey, s.Status, s.Attempt, s.MaxAttempts, s.NextAttemptAt, s.LockedUntil, s.LastError, s.IdempotencyKey, now, now)
	if err != nil {
		return nil, err
	}
	s.CreatedAt = now
	s.UpdatedAt = now
	return &s, nil
}
func (r *stepRepository) GetByKey(ctx context.Context, saga, key string) (*domain.Step, error) {
	var x stepRow
	err := r.db.GetContext(ctx, &x, `SELECT saga_id,step_key,status,attempt,max_attempts,next_attempt_at,locked_until,last_error,idempotency_key,created_at,updated_at FROM order_saga_steps WHERE saga_id=$1 AND step_key=$2`, saga, key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrStepNotFound
	}
	if err != nil {
		return nil, err
	}
	return step(x), nil
}
func (r *stepRepository) ListBySagaID(ctx context.Context, saga string) ([]domain.Step, error) {
	var xs []stepRow
	if err := r.db.SelectContext(ctx, &xs, `SELECT saga_id,step_key,status,attempt,max_attempts,next_attempt_at,locked_until,last_error,idempotency_key,created_at,updated_at FROM order_saga_steps WHERE saga_id=$1 ORDER BY step_key`, saga); err != nil {
		return nil, err
	}
	out := make([]domain.Step, 0, len(xs))
	for _, x := range xs {
		out = append(out, *step(x))
	}
	return out, nil
}
func (r *stepRepository) UpdateStatus(ctx context.Context, saga, key, status string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE order_saga_steps SET status=$1,updated_at=$2 WHERE saga_id=$3 AND step_key=$4`, status, time.Now().UTC(), saga, key)
	return err
}
