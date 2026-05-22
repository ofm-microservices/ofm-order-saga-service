package config

// AuthServiceConfig defines the outbound auth-service gRPC address.
type AuthServiceConfig struct {
	Address string `env:"ADDRESS" envDefault:"127.0.0.1:9501"`
}
