package zenhelper

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/minasoft-technology/zentask"
	"github.com/minasoft-technology/zentask/task"
	"github.com/nats-io/nats.go"
)

// ZenTask provides a simplified interface for zentask operations
type ZenTask struct {
	natsServer *zentask.NATSServer
	processor  *task.Processor
	repo       task.Repository
	logger     *slog.Logger
	ctx        context.Context
	cancel     context.CancelFunc
}

// NATSOptions holds optional NATS server configuration
type NATSOptions struct {
	Host      string // NATS server host (default: "0.0.0.0")
	Port      int    // NATS server port (default: 14222)
	HTTPHost  string // HTTP monitoring host (default: "0.0.0.0")
	HTTPPort  int    // HTTP monitoring port (default: 18222)
	StoreDir  string // Directory for storing NATS data (default: ~/.zentask/nats-data)
}

// Config holds the configuration for ZenTask
type Config struct {
	Username    string      // Required: Username for NATS authentication
	Password    string      // Required: Password for NATS authentication
	Logger      *slog.Logger // Optional: Logger instance (default: slog.Default)
	Debug       bool        // Optional: Enable debug logging
	NATSOptions *NATSOptions // Optional: NATS server configuration
}

// New creates a new ZenTask instance with all necessary components initialized
func New(cfg Config) (*ZenTask, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Use provided logger or create a default one
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Initialize NATS server configuration
	natsConfig := zentask.NATSConfig{
		Username: cfg.Username,
		Password: cfg.Password,
		Logger:   logger,
		Debug:    cfg.Debug,
	}

	// Apply optional NATS configuration if provided
	if cfg.NATSOptions != nil {
		if cfg.NATSOptions.Host != "" {
			natsConfig.NATSHost = cfg.NATSOptions.Host
		}
		if cfg.NATSOptions.Port != 0 {
			natsConfig.NATSPort = cfg.NATSOptions.Port
		}
		if cfg.NATSOptions.HTTPHost != "" {
			natsConfig.HTTPHost = cfg.NATSOptions.HTTPHost
		}
		if cfg.NATSOptions.HTTPPort != 0 {
			natsConfig.HTTPPort = cfg.NATSOptions.HTTPPort
		}
		if cfg.NATSOptions.StoreDir != "" {
			natsConfig.StoreDir = cfg.NATSOptions.StoreDir
		}
	}

	// Start NATS server
	natsServer, err := zentask.NewNATSServer(ctx, natsConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create NATS server: %w", err)
	}

	// Connect to NATS with authentication
	nc, err := nats.Connect(natsServer.GetConnectionURL(),
		nats.UserInfo(cfg.Username, cfg.Password))
	if err != nil {
		cancel()
		natsServer.Stop()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		cancel()
		nc.Close()
		natsServer.Stop()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Create task repository
	repo, err := task.NewJetStreamRepository(js)
	if err != nil {
		cancel()
		nc.Close()
		natsServer.Stop()
		return nil, fmt.Errorf("failed to create task repository: %w", err)
	}

	// Create task processor
	processor, err := task.NewProcessor(js, repo)
	if err != nil {
		cancel()
		nc.Close()
		natsServer.Stop()
		return nil, fmt.Errorf("failed to create task processor: %w", err)
	}

	// Start the processor
	if err := processor.Start(); err != nil {
		cancel()
		nc.Close()
		natsServer.Stop()
		return nil, fmt.Errorf("failed to start processor: %w", err)
	}

	return &ZenTask{
		natsServer: natsServer,
		processor:  processor,
		repo:       repo,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// AddHandler registers a new task handler
func (z *ZenTask) AddHandler(taskType string, handler task.Handler) error {
	return z.processor.RegisterHandler(taskType, handler)
}

// EnqueueTask creates and enqueues a new task
func (z *ZenTask) EnqueueTask(name string, taskType string, payload interface{}) (*task.Task, error) {
	// Create a new payload with the given type and data
	payloadData, err := task.NewPayload(taskType, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to create payload: %w", err)
	}

	// Create and enqueue the task
	newTask := task.NewTask(name).
		WithPayload(taskType, payloadData)

	// Build the task
	t, err := newTask.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build task: %w", err)
	}

	// Enqueue the task
	err = z.repo.Enqueue(z.ctx, t)
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue task: %w", err)
	}

	z.logger.Info("Task enqueued successfully", "taskId", t.ID, "type", taskType)
	return t, nil
}

// Stop gracefully shuts down all components
func (z *ZenTask) Stop() {
	z.cancel()
	z.natsServer.Stop()
}
