package config

// PaymentServiceConfig defines the outbound gRPC target for payment-service.
type PaymentServiceConfig struct {
	Address string `env:"ADDRESS,required"`
}
