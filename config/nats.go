package config

// NATSConfig is retained as a source-compatible alias for tests and adapters
// that have not yet been removed. Runtime order-saga configuration is KafkaConfig.
type NATSConfig = KafkaConfig
