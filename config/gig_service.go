package config

// GigServiceConfig defines the outbound gRPC target for gig-service.
type GigServiceConfig struct {
	Address string `env:"ADDRESS,required"`
}
