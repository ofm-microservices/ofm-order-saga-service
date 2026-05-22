package config

// FileServiceConfig defines the outbound gRPC target for file-service.
type FileServiceConfig struct {
	Address string `env:"ADDRESS,required"`
}
