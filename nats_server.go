package zentask

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/charmbracelet/log"
	"github.com/nats-io/nats-server/v2/server"
)

// Default configuration values
const (
	DefaultNATSPort = 4222
	DefaultNATSHost = "0.0.0.0"
	DefaultHTTPPort = 8222
	DefaultHTTPHost = "0.0.0.0"
)

// getDefaultStoreDir returns the default directory for storing NATS data
func getDefaultStoreDir() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "zentask", "nats-data")
	}
	return filepath.Join(homeDir, ".zentask", "nats-data")
}

// NATSConfig holds the configuration for embedded NATS server
type NATSConfig struct {
	NATSPort int    // Port for embedded NATS server (default: 4222)
	NATSHost string // Host for embedded NATS server (default: "0.0.0.0")
	HTTPPort int    // Port for monitoring HTTP (default: 8222)
	HTTPHost string // Host for monitoring HTTP (default: "0.0.0.0")
	StoreDir string // Directory for storing NATS data (default: ~/.zentask/nats-data)
	Debug    bool   // Enable debug logging for NATS server
	Trace    bool   // Enable trace logging for NATS server
	Logger   *log.Logger
}

// NATSServer represents the embedded NATS server
type NATSServer struct {
	server *server.Server
	config NATSConfig
	cancel context.CancelFunc
}

// NewNATSServer creates a new embedded NATS server
func NewNATSServer(ctx context.Context, config NATSConfig) (*NATSServer, error) {
	// Validate configuration
	if config.NATSPort < -1 {
		return nil, fmt.Errorf("invalid NATS port: %d", config.NATSPort)
	}
	if config.HTTPPort < -1 {
		return nil, fmt.Errorf("invalid HTTP port: %d", config.HTTPPort)
	}

	// Set default values
	if config.NATSHost == "" {
		config.NATSHost = DefaultNATSHost
	}
	if config.NATSPort == 0 {
		config.NATSPort = DefaultNATSPort
	}
	if config.HTTPHost == "" {
		config.HTTPHost = DefaultHTTPHost
	}
	if config.HTTPPort == 0 {
		config.HTTPPort = DefaultHTTPPort
	}
	if config.StoreDir == "" {
		config.StoreDir = getDefaultStoreDir()
	}

	// Create store directory if it doesn't exist
	if config.StoreDir != "" {
		if err := os.MkdirAll(config.StoreDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create store directory: %v", err)
		}
	}

	// Create server options
	opts := &server.Options{
		ServerName: "zentask-embedded-nats",
		Host:       config.NATSHost,
		Port:       config.NATSPort,
		HTTPHost:   config.HTTPHost,
		HTTPPort:   config.HTTPPort,
		JetStream:  true,
		StoreDir:   config.StoreDir,
		NoSigs:     true,
		Debug:      config.Debug,
		Trace:      config.Trace,
		NoLog:      config.Logger == nil,
	}

	// Use random ports if not specified
	if opts.Port == 0 {
		opts.Port = server.RANDOM_PORT
	}
	if opts.HTTPPort == 0 {
		opts.HTTPPort = server.RANDOM_PORT
	}

	// Create NATS server
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS server: %w", err)
	}

	// Set logger if provided
	if config.Logger != nil {
		srv.SetLogger(&NATSLogger{logger: config.Logger}, config.Debug, config.Trace)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(ctx)
	ns := &NATSServer{
		server: srv,
		config: config,
		cancel: cancel,
	}

	// Start the server in a goroutine
	go func() {
		srv.Start()
		<-ctx.Done()
	}()

	// Wait for server to be ready
	if !srv.ReadyForConnections(5 * time.Second) {
		cancel()
		return nil, fmt.Errorf("NATS server failed to start")
	}

	if config.Logger != nil {
		config.Logger.Info("NATS server is running",
			"url", srv.ClientURL(),
			"monitoring", fmt.Sprintf("http://%s:%d", config.HTTPHost, srv.MonitorAddr().Port))
	}

	return ns, nil
}

// Stop gracefully stops the NATS server
func (ns *NATSServer) Stop() {
	if ns.cancel != nil {
		ns.cancel()
	}
	// Give the server a moment to process the cancellation
	time.Sleep(100 * time.Millisecond)
	if ns.server != nil {
		ns.server.Shutdown()
	}
}

// GetConnectionURL returns the URL for connecting to this NATS server
func (ns *NATSServer) GetConnectionURL() string {
	if ns.server == nil {
		return ""
	}
	addr := ns.server.ClientURL()
	return addr
}

// NATSLogger implements the NATS server.Logger interface
type NATSLogger struct {
	logger *log.Logger
}

func (l *NATSLogger) Noticef(format string, v ...interface{}) {
	l.logger.Info(fmt.Sprintf(format, v...))
}

func (l *NATSLogger) Warnf(format string, v ...interface{}) {
	l.logger.Warn(fmt.Sprintf(format, v...))
}

func (l *NATSLogger) Fatalf(format string, v ...interface{}) {
	l.logger.Error(fmt.Sprintf(format, v...))
	os.Exit(1)
}

func (l *NATSLogger) Errorf(format string, v ...interface{}) {
	l.logger.Error(fmt.Sprintf(format, v...))
}

func (l *NATSLogger) Debugf(format string, v ...interface{}) {
	l.logger.Debug(fmt.Sprintf(format, v...))
}

func (l *NATSLogger) Tracef(format string, v ...interface{}) {
	l.logger.Debug(fmt.Sprintf(format, v...))
}
