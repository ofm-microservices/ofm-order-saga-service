package config

// GRPCConfig defines the checkout gRPC listener settings.
type GRPCConfig struct {
	Host string `env:"GRPC_HOST" envDefault:"0.0.0.0"`
	Port int    `env:"GRPC_PORT" envDefault:"9507"`
}
