package config

import "github.com/caarlos0/env/v11"

// Config groups the environment-backed settings for the order saga service.
type Config struct {
	App     AppConfig            `envPrefix:"APP_"`
	GRPC    GRPCConfig           `envPrefix:"GRPC_"`
	Scylla  ScyllaConfig         `envPrefix:"SCYLLA_"`
	Auth    AuthServiceConfig    `envPrefix:"AUTH_SERVICE_"`
	Gig     GigServiceConfig     `envPrefix:"GIG_SERVICE_"`
	Order   OrderServiceConfig   `envPrefix:"ORDER_SERVICE_"`
	Payment PaymentServiceConfig `envPrefix:"PAYMENT_SERVICE_"`
	File    FileServiceConfig    `envPrefix:"FILE_SERVICE_"`
	Kafka   KafkaConfig          `envPrefix:"KAFKA_"`
	Metrics MetricsConfig
}

// MetricsConfig controls the Prometheus endpoint owned by order-saga-service.
type MetricsConfig struct {
	Enabled bool   `env:"METRICS_ENABLED" envDefault:"true"`
	Host    string `env:"METRICS_HOST" envDefault:"0.0.0.0"`
	Port    int    `env:"METRICS_PORT" envDefault:"9607"`
	Path    string `env:"METRICS_PATH" envDefault:"/metrics"`
}

// Load parses the service config from environment variables.
func Load() (*Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
