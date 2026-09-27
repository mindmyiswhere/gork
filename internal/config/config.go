package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config — конфигурация оркестратора, собранная из переменных окружения.
type Config struct {
	GRPC  GRPCConfig
	Redis RedisConfig
	Log   LogConfig
}

type GRPCConfig struct {
	Host            string
	Port            int
	ShutdownTimeout time.Duration
}

// Addr возвращает адрес в формате "host:port".
func (c GRPCConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type LogConfig struct {
	Level  string // debug | info | warn | error
	Format string // text | json
}

// Load читает конфиг из переменных окружения.
// Предполагается, что .env уже загружен в окружение (см. cmd/orchestrator/main.go).
func Load() (*Config, error) {
	cfg := &Config{
		GRPC: GRPCConfig{
			Host:            getEnv("GORK_GRPC_HOST", "0.0.0.0"),
			Port:            getEnvInt("GORK_GRPC_PORT", 50051),
			ShutdownTimeout: getEnvDuration("GORK_GRPC_SHUTDOWN_TIMEOUT", 10*time.Second),
		},
		Redis: RedisConfig{
			Addr:     getEnv("GORK_REDIS_ADDR", "localhost:6379"),
			Password: getEnv("GORK_REDIS_PASSWORD", ""),
			DB:       getEnvInt("GORK_REDIS_DB", 0),
		},
		Log: LogConfig{
			Level:  getEnv("GORK_LOG_LEVEL", "info"),
			Format: getEnv("GORK_LOG_FORMAT", "text"),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.GRPC.Port <= 0 || c.GRPC.Port > 65535 {
		return fmt.Errorf("invalid GORK_GRPC_PORT: %d", c.GRPC.Port)
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("GORK_REDIS_ADDR must not be empty")
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("invalid GORK_LOG_LEVEL: %s", c.Log.Level)
	}
	switch c.Log.Format {
	case "text", "json":
	default:
		return fmt.Errorf("invalid GORK_LOG_FORMAT: %s", c.Log.Format)
	}
	return nil
}

// ===== helpers =====

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
