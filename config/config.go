package config

import (
	"os"
	"time"
)

// Config holds all configuration for the application
type Config struct {
	NATS NATSConfig
}

// NATSConfig holds NATS-specific configuration
type NATSConfig struct {
	URL               string
	ConnectTimeout    time.Duration
	MaxReconnectTries int
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		NATS: NATSConfig{
			URL:               getEnvOrDefault("NATS_URL", "nats://localhost:4222"),
			ConnectTimeout:    10 * time.Second,
			MaxReconnectTries: 5,
		},
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
