package config

import "time"

// PullConsumerConfig defines the runtime settings for one JetStream pull consumer.
type PullConsumerConfig struct {
	Stream     string
	Subject    string
	Durable    string
	BatchSize  int
	MaxWait    time.Duration
	Workers    int
	QueueSize  int
	AckWait    time.Duration
	MaxDeliver int
}
