package config

// OrderServiceConfig defines the outbound gRPC target for order-service.
type OrderServiceConfig struct {
	Address string `env:"ADDRESS,required"`
}
