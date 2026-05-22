package eventbroker

import (
	"context"

	"order-saga-service/config"
)

// MessageHandler processes one broker message.
type MessageHandler func(ctx context.Context, subject string, payload []byte) error

// EventBroker abstracts message publication and subscription.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error
	RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler MessageHandler) error
	Close()
}
