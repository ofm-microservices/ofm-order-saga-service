package config

import "time"

// DBConfig defines the PostgreSQL database owned by order-saga-service.
type DBConfig struct {
	Host            string        `env:"HOST" envDefault:"127.0.0.1"`
	Port            int           `env:"PORT" envDefault:"5441"`
	User            string        `env:"USER" envDefault:"admin"`
	Password        string        `env:"PASSWORD" envDefault:"admin"`
	Name            string        `env:"NAME" envDefault:"order_saga"`
	SSLMode         string        `env:"SSLMODE" envDefault:"disable"`
	MigrationsPath  string        `env:"MIGRATIONS_PATH" envDefault:"file://migration/postgres"`
	MigrationsTable string        `env:"MIGRATIONS_TABLE" envDefault:"schema_migrations_order_saga"`
	MaxOpenConns    int           `env:"MAX_OPEN_CONNS" envDefault:"20"`
	MaxIdleConns    int           `env:"MAX_IDLE_CONNS" envDefault:"10"`
	ConnMaxLifetime time.Duration `env:"CONN_MAX_LIFETIME" envDefault:"5m"`
}
