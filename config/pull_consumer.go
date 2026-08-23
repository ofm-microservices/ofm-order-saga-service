package config

import "time"

// PullConsumerConfig defines runtime settings for one Kafka pull consumer.
type PullConsumerConfig struct {
	Stream            string
	Subject           string
	Durable           string
	BatchSize         int
	MaxWait           time.Duration
	Workers           int
	QueueSize         int
	AckWait           time.Duration
	MaxDeliver        int
	DeadLetterSubject string
}
