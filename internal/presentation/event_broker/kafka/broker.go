package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
	"order-saga-service/config"
	eb "order-saga-service/internal/presentation/event_broker"
)

type broker struct {
	brokers           []string
	group, deadLetter string
	mu                sync.Mutex
	readers           []*kafka.Reader
}

// NewBroker constructs the Kafka event broker used by the order saga.
func NewBroker(cfg config.KafkaConfig) (eb.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are empty")
	}
	return &broker{brokers: cfg.Brokers, group: cfg.GroupID, deadLetter: cfg.GroupID + ".dead-letter"}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject}
	defer w.Close()
	return w.WriteMessages(ctx, kafka.Message{Value: payload, Headers: kafkaHeaders(ctx)})
}

func kafkaHeaders(ctx context.Context) []kafka.Header {
	headers := make([]kafka.Header, 0, 5)
	for key, value := range requestmetadata.OutgoingHeaders(ctx) {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

func (b *broker) Subscribe(ctx context.Context, subject string, handler eb.MessageHandler) error {
	return b.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: subject}, handler)
}

func (b *broker) RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler eb.MessageHandler) error {
	// Each saga topic needs its own consumer group. Reusing one group for all
	// command/result topics leaves kafka-go with stale coordinator metadata after
	// broker changes and can surface as attempts to dial 127.0.0.1:9092.
	groupID := strings.TrimSpace(b.group) + "-" + strings.TrimSpace(cfg.Subject)
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: cfg.Subject, GroupID: groupID, MinBytes: 1, MaxBytes: 10e6})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	defer r.Close()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		attempts := 0
		err = resilience.Retry(ctx, resilience.RetryPolicyFromEnv(), func(attemptCtx context.Context, attempt int) error {
			attempts = attempt
			return handler(kafkaprop.Context(attemptCtx, msg.Headers), cfg.Subject, msg.Value)
		})
		if err != nil {
			payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: cfg.Subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", err), Error: err.Error(), FailedAt: time.Now().UTC()})
			if marshalErr != nil {
				return marshalErr
			}
			writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second}
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			dlqErr := writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
			cancel()
			_ = writer.Close()
			if dlqErr != nil {
				return dlqErr
			}
			sharedmetrics.IncKafkaDLQ(b.deadLetter)
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
