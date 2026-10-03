package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	commonevents "github.com/ofm-microservices/ofm-common/pkg/events"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	transportkafka "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
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

const recoveryMaxAttempts = 6

// NewBroker constructs the Kafka event broker used by the order saga.
func NewBroker(cfg config.KafkaConfig) (eb.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are empty")
	}
	return &broker{brokers: cfg.Brokers, group: cfg.GroupID, deadLetter: cfg.GroupID + ".dead-letter"}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	enveloped, _, err := commonevents.Wrap(subject, payload)
	if err != nil {
		return err
	}
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, BatchSize: 100, BatchTimeout: 50 * time.Millisecond}
	defer w.Close()
	transportkafka.Published(subject, enveloped)
	return w.WriteMessages(ctx, kafka.Message{Value: enveloped, Headers: kafkaHeaders(ctx)})
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
	if strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
		if err := ensureRecoveryTopic(ctx, b.brokers, cfg.Subject); err != nil {
			return fmt.Errorf("ensure recovery topic %q: %w", cfg.Subject, err)
		}
		if strings.TrimSpace(cfg.DeadLetterSubject) != "" {
			if err := ensureRecoveryTopic(ctx, b.brokers, cfg.DeadLetterSubject); err != nil {
				return fmt.Errorf("ensure recovery dead-letter topic %q: %w", cfg.DeadLetterSubject, err)
			}
		}
	}
	// Each saga topic needs its own consumer group. Reusing one group for all
	// command/result topics leaves kafka-go with stale coordinator metadata after
	// broker changes and can surface as attempts to dial 127.0.0.1:9092.
	groupID := strings.TrimSpace(b.group) + "-" + strings.TrimSpace(cfg.Subject)
	if strings.TrimSpace(cfg.GroupID) != "" && !strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
		groupID = strings.TrimSpace(cfg.GroupID)
		return b.runExplicitConsumerGroup(ctx, cfg, handler, groupID)
	}
	if strings.TrimSpace(cfg.GroupID) != "" {
		groupID = strings.TrimSpace(cfg.GroupID)
	}
	go func() {
		_ = (resilience.KafkaRetryQueueConfig{Brokers: b.brokers, Group: groupID, MaxAttempts: resilience.DefaultRetryPolicy.MaxAttempts}).Run(ctx)
	}()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     b.brokers,
		Dialer:      &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true},
		Topic:       cfg.Subject,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     50 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	defer r.Close()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		attempts := retryAttempt(msg.Headers)
		payload, _, unwrapErr := commonevents.Unwrap(msg.Value)
		if unwrapErr != nil {
			return b.deadLetterMessage(ctx, cfg.Subject, msg, attempts, unwrapErr)
		}
		transportkafka.Consumed(cfg.Subject, msg.Partition, msg.Offset, attempts, payload)
		if strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
			payload = msg.Value
		}
		err = handler(kafkaprop.Context(ctx, msg.Headers), cfg.Subject, payload)
		if err != nil {
			var permanent resilience.PermanentError
			if !errors.As(err, &permanent) && attempts < resilience.DefaultRetryPolicy.MaxAttempts {
				writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: resilience.RetryTopic(groupID), WriteTimeout: 5 * time.Second}
				queueErr := (resilience.KafkaRetryQueue{Writer: writer}).Enqueue(ctx, msg, cfg.Subject, attempts+1, err)
				_ = writer.Close()
				if queueErr != nil {
					return queueErr
				}
				continue
			}
			deadLetterTopic := b.deadLetter
			if strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
				deadLetterTopic = strings.TrimSpace(cfg.GroupID) + ".dead-letter"
			} else if strings.TrimSpace(cfg.DeadLetterSubject) != "" {
				deadLetterTopic = strings.TrimSpace(cfg.DeadLetterSubject)
			}
			payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: cfg.Subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", err), Error: err.Error(), FailedAt: time.Now().UTC()})
			if marshalErr != nil {
				return marshalErr
			}
			writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: deadLetterTopic, WriteTimeout: 5 * time.Second}
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			dlqErr := writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
			cancel()
			_ = writer.Close()
			if dlqErr != nil {
				return dlqErr
			}
			sharedmetrics.IncKafkaDLQ(deadLetterTopic)
			log.Printf("order-saga recovery moved to DLQ topic=%s dead_letter_topic=%s offset=%d error=%v", cfg.Subject, deadLetterTopic, msg.Offset, err)
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func retryAttempt(headers []kafka.Header) int {
	for _, h := range headers {
		if h.Key == "x-ofm-retry-attempt" {
			if n, err := strconv.Atoi(string(h.Value)); err == nil && n > 0 {
				return n
			}
		}
	}
	return 1
}

func (b *broker) deadLetterMessage(ctx context.Context, subject string, msg kafka.Message, attempts int, cause error) error {
	topic := b.deadLetter
	if strings.HasPrefix(subject, "migration.recovery.commands.") {
		topic = strings.TrimSpace(subject) + ".dead-letter"
	}
	payload, err := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", cause), Error: cause.Error(), FailedAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: topic, WriteTimeout: 5 * time.Second}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
	cancel()
	_ = writer.Close()
	if err != nil {
		return err
	}
	sharedmetrics.IncKafkaDLQ(topic)
	return nil
}

func (b *broker) runExplicitConsumerGroup(ctx context.Context, cfg config.PullConsumerConfig, handler eb.MessageHandler, groupID string) error {
	cg, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		Brokers:     b.brokers,
		Dialer:      &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true},
		Topics:      []string{cfg.Subject},
		ID:          groupID,
		StartOffset: kafka.FirstOffset,
	})
	if err != nil {
		return fmt.Errorf("create Kafka consumer group topic=%s group=%s: %w", cfg.Subject, groupID, err)
	}
	defer cg.Close()
	for {
		generation, nextErr := cg.Next(ctx)
		if nextErr != nil {
			return fmt.Errorf("get Kafka consumer generation topic=%s group=%s: %w", cfg.Subject, groupID, nextErr)
		}
		generation.Start(func(genCtx context.Context) {
			// More replicas than partitions is valid. Keep idle members in the
			// generation instead of immediately rejoining in a CPU-heavy loop.
			if len(generation.Assignments[cfg.Subject]) == 0 {
				<-genCtx.Done()
				return
			}
			var workers sync.WaitGroup
			for _, assignment := range generation.Assignments[cfg.Subject] {
				assignment := assignment
				workers.Add(1)
				go func() {
					defer workers.Done()
					reader := kafka.NewReader(kafka.ReaderConfig{
						Brokers: b.brokers, Topic: cfg.Subject, Partition: assignment.ID,
						MaxWait:  50 * time.Millisecond,
						MinBytes: 1, MaxBytes: 10e6,
					})
					defer reader.Close()
					if assignment.Offset >= 0 {
						if setErr := reader.SetOffset(assignment.Offset); setErr != nil {
							log.Printf("order saga recovery set offset failed topic=%s partition=%d: %v", cfg.Subject, assignment.ID, setErr)
							return
						}
					}
					for genCtx.Err() == nil {
						message, fetchErr := reader.FetchMessage(genCtx)
						if fetchErr != nil {
							return
						}
						handleErr := resilience.Retry(genCtx, resilience.RetryPolicy{MaxAttempts: 1}, func(attemptCtx context.Context, attempt int) error {
							messageCtx, cancel := context.WithTimeout(context.WithoutCancel(attemptCtx), 30*time.Second)
							defer cancel()
							log.Printf("order-saga recovery attempt topic=%s attempt=%d offset=%d", cfg.Subject, attempt, message.Offset)
							payload, _, unwrapErr := commonevents.Unwrap(message.Value)
							if unwrapErr != nil {
								return unwrapErr
							}
							if strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
								payload = message.Value
							}
							return handler(kafkaprop.Context(messageCtx, message.Headers), cfg.Subject, payload)
						})
						if handleErr != nil {
							deadLetterTopic := b.deadLetter
							if strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
								deadLetterTopic = strings.TrimSpace(cfg.GroupID) + ".dead-letter"
							}
							payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{
								OriginalKey: message.Key, OriginalValue: message.Value, OriginalTopic: message.Topic,
								OriginalPartition: message.Partition, OriginalOffset: message.Offset,
								ErrorClass: fmt.Sprintf("%T", handleErr), Error: handleErr.Error(), FailedAt: time.Now().UTC(),
							})
							if marshalErr == nil {
								writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: deadLetterTopic, Balancer: &kafka.Hash{}, WriteTimeout: 5 * time.Second}
								writeCtx, cancel := context.WithTimeout(context.WithoutCancel(genCtx), 5*time.Second)
								_ = writer.WriteMessages(writeCtx, kafka.Message{Key: message.Key, Value: payload, Headers: message.Headers})
								cancel()
								_ = writer.Close()
								sharedmetrics.IncKafkaDLQ(deadLetterTopic)
								log.Printf("order-saga recovery moved to DLQ topic=%s dead_letter_topic=%s error=%v", cfg.Subject, deadLetterTopic, handleErr)
							}
						}
						if commitErr := generation.CommitOffsets(map[string]map[int]int64{cfg.Subject: {assignment.ID: message.Offset + 1}}); commitErr != nil {
							return
						}
					}
				}()
			}
			workers.Wait()
		})
	}
}

// ensureRecoveryTopic closes the startup race between Kafka topic provisioning
// and a service recovering after a host or k3d restart. The operation is
// idempotent: Kafka returns TopicAlreadyExists when the infrastructure has
// already created the topic.
func ensureRecoveryTopic(ctx context.Context, brokers []string, topic string) error {
	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	err = conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 16, ReplicationFactor: 1})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return err
	}
	return nil
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
