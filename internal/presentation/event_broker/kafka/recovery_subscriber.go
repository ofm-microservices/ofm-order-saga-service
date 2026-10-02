package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
)

// RecoverySubscriber applies order-saga fallback commands through its application service.
type RecoverySubscriber interface{ Subscribe(context.Context) error }
type recoverySubscriber struct {
	broker app.EventBroker
	svc    app.Service
	cfg    config.KafkaConfig
	log    logging.Logger
}

// NewRecoverySubscriber constructs the order-saga recovery Kafka adapter.
func NewRecoverySubscriber(b app.EventBroker, svc app.Service, cfg config.KafkaConfig, log logging.Logger) (RecoverySubscriber, error) {
	if b == nil || svc == nil || log == nil {
		return nil, errors.New("invalid order saga recovery subscriber dependency")
	}
	return &recoverySubscriber{broker: b, svc: svc, cfg: cfg, log: log.With(logging.String("module", "kafka-order-saga-recovery-subscriber"))}, nil
}
func (s *recoverySubscriber) Subscribe(ctx context.Context) error {
	s.log.Info("starting order-saga recovery Kafka consumer",
		logging.String("topic", s.cfg.RecoveryTopic),
		logging.String("group", s.cfg.RecoveryGroup),
		logging.String("brokers", strings.Join(s.cfg.Brokers, ",")),
	)
	err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Subject:           s.cfg.RecoveryTopic,
		GroupID:           s.cfg.RecoveryGroup,
		DeadLetterSubject: s.cfg.DeadLetterTopic,
	}, s.handle)
	if err != nil {
		s.log.Error("order-saga recovery Kafka consumer stopped",
			logging.String("topic", s.cfg.RecoveryTopic),
			logging.String("group", s.cfg.RecoveryGroup),
			logging.Err(err),
		)
		return err
	}
	s.log.Info("order-saga recovery Kafka consumer stopped cleanly",
		logging.String("topic", s.cfg.RecoveryTopic),
		logging.String("group", s.cfg.RecoveryGroup),
	)
	return nil
}
func (s *recoverySubscriber) handle(ctx context.Context, _ string, raw []byte) error {
	var cmd events.Envelope
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return fmt.Errorf("decode order saga recovery command: %w", err)
	}
	if !strings.EqualFold(cmd.AggregateType, "order_saga") {
		return fmt.Errorf("unsupported order saga recovery aggregate_type=%q", cmd.AggregateType)
	}
	if strings.TrimSpace(cmd.AggregateID) == "" {
		return resilience.Permanent(fmt.Errorf("order-saga recovery command %s is missing aggregate_id", cmd.CommandID))
	}
	var p app.StartOrderCommand
	if err := json.Unmarshal(cmd.Payload, &p); err != nil {
		return err
	}
	if strings.TrimSpace(p.OrderID) == "" {
		return resilience.Permanent(fmt.Errorf("order-saga recovery command %s is missing order_id", cmd.CommandID))
	}
	if strings.TrimSpace(p.SagaID) == "" {
		return resilience.Permanent(fmt.Errorf("order-saga recovery command %s is missing saga_id", cmd.CommandID))
	}
	p.IdempotencyKey = cmd.IdempotencyKey
	result, err := s.svc.StartOrder(ctx, p)
	if err != nil {
		s.log.Error("order-saga recovery command failed",
			logging.String("operation", "order_saga.recovery.start"),
			logging.String("command_id", cmd.CommandID),
			logging.String("event_id", cmd.EventID),
			logging.String("order_id", p.OrderID),
			logging.String("buyer_id", p.BuyerID),
			logging.String("gig_id", p.GigID),
			logging.String("package_id", p.PackageID),
			logging.Err(err),
		)
		if errors.Is(err, app.ErrSelfOrderNotAllowed) {
			return resilience.Permanent(err)
		}
		return err
	}
	body, e := json.Marshal(events.Envelope{EventID: cmd.EventID + ".completed", CommandID: cmd.CommandID, CorrelationID: cmd.CorrelationID, CausationID: cmd.EventID, IdempotencyKey: cmd.IdempotencyKey, TestRunID: cmd.TestRunID, EventType: "migration.recovery.completed", Operation: cmd.Operation, SchemaVersion: 1, AggregateType: "order_saga", AggregateID: cmd.AggregateID, SourceService: "order-saga-service-recovery", OccurredAt: time.Now().UTC(), Payload: mustJSON(result)})
	if e != nil {
		return e
	}
	return s.broker.Publish(context.WithoutCancel(ctx), s.cfg.RecoveryCompletedTopic, body)
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
