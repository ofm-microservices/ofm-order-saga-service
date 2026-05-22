package scylla

import "time"

// SessionRow is the Scylla persistence model for order saga sessions.
type SessionRow struct {
	SagaID               string
	OrderID              string
	BuyerID              string
	SellerID             string
	BuyerEmail           string
	RealtimeConnectionID string
	GigID                string
	GigTitle             string
	PackageID            string
	PackageTier          string
	PackageDescription   string
	PackageDeliveryDays  int32
	PriceCents           int64
	Currency             string
	Status               string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// StepRow is the Scylla persistence model for order saga steps.
type StepRow struct {
	SagaID    string
	StepKey   string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}
