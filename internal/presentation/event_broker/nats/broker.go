package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	eb "order-saga-service/internal/presentation/event_broker"
)

type broker struct {
	conn *nats.Conn
	log  logging.Logger
}

// NewBroker connects to NATS and returns the runtime broker adapter.
func NewBroker(cfg config.NATSConfig, log logging.Logger) (eb.EventBroker, error) {
	if log == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	opts := []nats.Option{nats.Timeout(5 * time.Second)}
	if cfg.User != "" || cfg.Password != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	conn, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, err
	}
	return &broker{conn: conn, log: log.With(logging.String("module", "nats"))}, nil
}

// Publish writes one command or event to NATS.
func (b *broker) Publish(_ context.Context, subject string, payload []byte) error {
	if err := b.conn.Publish(subject, payload); err != nil {
		return err
	}
	return b.conn.Flush()
}

// Subscribe starts a simple synchronous subscription.
func (b *broker) Subscribe(_ context.Context, subject string, handler eb.MessageHandler) error {
	_, err := b.conn.Subscribe(subject, func(msg *nats.Msg) {
		_ = handler(context.Background(), msg.Subject, msg.Data)
	})
	if err != nil {
		return err
	}
	return b.conn.Flush()
}

// RunPullConsumer starts a JetStream-backed pull consumer.
func (b *broker) RunPullConsumer(_ context.Context, cfg config.PullConsumerConfig, handler eb.MessageHandler) error {
	js, err := b.conn.JetStream()
	if err != nil {
		return err
	}
	consumerCfg := &nats.ConsumerConfig{
		Durable:       cfg.Durable,
		FilterSubject: cfg.Subject,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
	}
	if _, err := js.AddConsumer(cfg.Stream, consumerCfg); err != nil {
		if _, updateErr := js.UpdateConsumer(cfg.Stream, consumerCfg); updateErr != nil {
			return fmt.Errorf("ensure consumer %s on stream %s: %w; update err: %v", cfg.Durable, cfg.Stream, err, updateErr)
		}
	}
	sub, err := js.PullSubscribe(cfg.Subject, cfg.Durable, nats.BindStream(cfg.Stream), nats.ManualAck())
	if err != nil {
		return err
	}
	go func() {
		for {
			msgs, fetchErr := sub.Fetch(cfg.BatchSize, nats.MaxWait(cfg.MaxWait))
			if fetchErr != nil {
				if fetchErr == nats.ErrTimeout {
					continue
				}
				b.log.Error("pull consumer fetch failed", logging.String("subject", cfg.Subject), logging.Err(fetchErr))
				time.Sleep(250 * time.Millisecond)
				continue
			}
			for _, msg := range msgs {
				if err := handler(context.Background(), msg.Subject, msg.Data); err != nil {
					b.log.Error("pull consumer handler failed", logging.String("subject", msg.Subject), logging.Err(err))
					_ = msg.Nak()
					continue
				}
				_ = msg.Ack()
			}
		}
	}()
	return nil
}

// Close releases the NATS connection.
func (b *broker) Close() {
	if b.conn != nil {
		b.conn.Close()
	}
}
