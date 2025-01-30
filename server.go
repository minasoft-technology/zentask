package zentask

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Server represents the ZenTask server that processes tasks
type Server struct {
	nc           *nats.Conn
	js           nats.JetStreamContext
	config       ServerConfig
	handlers     map[string]TaskHandler
	workers      map[string]*WorkerPool
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	streamName   string
	subscriptions map[string]*nats.Subscription
}

// ServerConfig holds the configuration for ZenTask server
type ServerConfig struct {
	NatsURL        string
	Queues         []QueueConfig
	ErrorLogger    *log.Logger
	StreamName     string        // Name of the JetStream stream
	StreamReplicas int          // Number of replicas for the stream
	StreamMaxAge   time.Duration // Maximum age of messages in the stream
	DLQEnabled     bool         // Enable Dead Letter Queue
	DLQRetention   time.Duration // How long to keep messages in DLQ
}

// QueueConfig holds the configuration for a specific queue
type QueueConfig struct {
	Name           string
	WorkerCount    int
	MaxRetries     int
	RetryBackoff   time.Duration
	MaxAckPending  int           // Maximum number of pending acknowledgments
	AckWait        time.Duration // How long to wait for an acknowledgment
	DLQSubject     string        // Subject for Dead Letter Queue (optional)
}

// TaskHandler is a function that processes a task
type TaskHandler func(context.Context, *Task) error

// NewServer creates a new ZenTask server
func NewServer(config ServerConfig) (*Server, error) {
	if config.NatsURL == "" {
		return nil, fmt.Errorf("NATS URL is required")
	}

	// Set default values
	if config.StreamName == "" {
		config.StreamName = "ZENTASK"
	}
	if config.StreamReplicas == 0 {
		config.StreamReplicas = 1
	}
	if config.StreamMaxAge == 0 {
		config.StreamMaxAge = 24 * time.Hour // Default to 24 hours
	}

	// Set default DLQ retention if enabled but not set
	if config.DLQEnabled && config.DLQRetention == 0 {
		config.DLQRetention = 7 * 24 * time.Hour // Default 7 days
	}

	// Connect to NATS
	nc, err := nats.Connect(config.NatsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %v", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %v", err)
	}

	// Create stream if it doesn't exist
	streamConfig := &nats.StreamConfig{
		Name:        config.StreamName,
		Subjects:    []string{fmt.Sprintf("%s.>", config.StreamName)},
		Retention:   nats.WorkQueuePolicy,
		MaxAge:      config.StreamMaxAge,
		Replicas:    config.StreamReplicas,
		Storage:     nats.FileStorage,
		Discard:     nats.DiscardOld,
		MaxMsgs:     -1,
		MaxBytes:    -1,
		Description: "ZenTask persistent task queue",
	}

	_, err = js.AddStream(streamConfig)
	if err != nil {
		// If stream already exists, update it
		if err == nats.ErrStreamNameAlreadyInUse {
			_, err = js.UpdateStream(streamConfig)
			if err != nil {
				nc.Close()
				return nil, fmt.Errorf("failed to update stream: %v", err)
			}
		} else {
			nc.Close()
			return nil, fmt.Errorf("failed to create stream: %v", err)
		}
	}

	// Create DLQ streams if enabled
	if config.DLQEnabled {
		dlqConfig := &nats.StreamConfig{
			Name:        fmt.Sprintf("%s_DLQ", config.StreamName),
			Subjects:    []string{fmt.Sprintf("%s.dlq.>", config.StreamName)},
			Retention:   nats.LimitsPolicy,
			MaxAge:      config.DLQRetention,
			Storage:     nats.FileStorage,
			Discard:     nats.DiscardOld,
			Description: "ZenTask Dead Letter Queue",
		}
		
		_, err = js.AddStream(dlqConfig)
		if err != nil && err != nats.ErrStreamNameAlreadyInUse {
			nc.Close()
			return nil, fmt.Errorf("failed to create DLQ stream: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		nc:           nc,
		js:           js,
		config:       config,
		handlers:     make(map[string]TaskHandler),
		workers:      make(map[string]*WorkerPool),
		ctx:          ctx,
		cancel:       cancel,
		streamName:   config.StreamName,
		subscriptions: make(map[string]*nats.Subscription),
	}

	return s, nil
}

// RegisterHandler registers a task handler for a specific queue
func (s *Server) RegisterHandler(queue string, handler TaskHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	s.handlers[queue] = handler
	return nil
}

// Start starts processing tasks from all configured queues
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Initialize maps if not already done
	if s.workers == nil {
		s.workers = make(map[string]*WorkerPool)
	}
	if s.subscriptions == nil {
		s.subscriptions = make(map[string]*nats.Subscription)
	}

	// Create context if not already done
	if s.ctx == nil {
		s.ctx, s.cancel = context.WithCancel(context.Background())
	}

	// Start each queue
	for _, qConfig := range s.config.Queues {
		if err := s.startQueue(qConfig); err != nil {
			return fmt.Errorf("failed to start queue %s: %v", qConfig.Name, err)
		}
		
		// Log queue start
		s.logError("Started queue", fmt.Errorf("queue: %s", qConfig.Name))
	}

	// Wait a bit for consumers to be ready
	time.Sleep(500 * time.Millisecond)

	return nil
}

func (s *Server) startQueue(qConfig QueueConfig) error {
	handler, exists := s.handlers[qConfig.Name]
	if !exists {
		return fmt.Errorf("no handler registered for queue: %s", qConfig.Name)
	}

	// Set default values for queue config
	if qConfig.MaxAckPending == 0 {
		qConfig.MaxAckPending = 1000
	}
	if qConfig.AckWait == 0 {
		qConfig.AckWait = 30 * time.Second
	}

	// Create durable consumer for the queue
	consumerConfig := &nats.ConsumerConfig{
		Durable:        qConfig.Name,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        qConfig.AckWait,
		MaxAckPending:  qConfig.MaxAckPending,
		FilterSubject:  fmt.Sprintf("%s.tasks.%s.>", s.streamName, qConfig.Name),
	}

	// Create or update consumer
	_, err := s.js.AddConsumer(s.streamName, consumerConfig)
	if err != nil {
		// Try to update if consumer already exists
		_, err = s.js.UpdateConsumer(s.streamName, consumerConfig)
		if err != nil {
			return fmt.Errorf("failed to create/update consumer: %v", err)
		}
	}

	// Subscribe to the delivery subject
	sub, err := s.js.PullSubscribe(
		fmt.Sprintf("%s.tasks.%s.>", s.streamName, qConfig.Name),
		qConfig.Name,
		nats.Bind(s.streamName, qConfig.Name),
	)
	if err != nil {
		return fmt.Errorf("failed to create subscription: %v", err)
	}

	s.subscriptions[qConfig.Name] = sub

	// Create worker pool
	pool := newWorkerPool(s.ctx, qConfig.WorkerCount)
	s.workers[qConfig.Name] = pool

	// Start workers to process messages
	for i := 0; i < qConfig.WorkerCount; i++ {
		go func() {
			for {
				select {
				case <-s.ctx.Done():
					return
				default:
					// Fetch one message at a time for better control
					msgs, err := sub.Fetch(1, nats.Context(s.ctx))
					if err != nil {
						if err != context.Canceled && err != context.DeadlineExceeded {
							s.logError("failed to fetch messages", err)
						}
						time.Sleep(100 * time.Millisecond) // Add small delay on error
						continue
					}

					// Process the message
					if len(msgs) > 0 {
						msg := msgs[0]
						// Process in the current goroutine for better control
						s.processMessage(msg, handler)
					}
				}
			}
		}()
	}

	s.logError("Started workers", fmt.Errorf("queue: %s, count: %d", qConfig.Name, qConfig.WorkerCount))

	return nil
}

func (s *Server) processMessage(msg *nats.Msg, handler TaskHandler) {
	var taskMsg taskMessage
	if err := json.Unmarshal(msg.Data, &taskMsg); err != nil {
		s.logError("Failed to unmarshal task", err)
		return
	}

	s.logError("Processing message", fmt.Errorf("queue: %s, id: %s", taskMsg.Queue, taskMsg.ID))

	// Create task from message
	task := &Task{
		Queue:    taskMsg.Queue,
		Metadata: taskMsg.Metadata,
	}

	// Unmarshal payload if present
	if len(taskMsg.Payload) > 0 {
		var payload interface{}
		if err := json.Unmarshal(taskMsg.Payload, &payload); err != nil {
			s.logError("Failed to unmarshal payload", err)
			msg.Nak() // Negative acknowledgment
			return
		}
		task.Payload = payload
	}

	// Set options if any are present
	if taskMsg.MaxRetries > 0 || taskMsg.RetryDelay > 0 || !taskMsg.ProcessAt.IsZero() ||
		taskMsg.Priority != 0 || taskMsg.UniqueKey != "" || taskMsg.GroupID != "" {
		task.Options = &TaskOptions{
			MaxRetries:  taskMsg.MaxRetries,
			RetryDelay:  taskMsg.RetryDelay,
			ProcessAt:   taskMsg.ProcessAt,
			Priority:    taskMsg.Priority,
			UniqueKey:   taskMsg.UniqueKey,
			GroupID:     taskMsg.GroupID,
		}
	}

	// Create context with timeout if specified
	ctx := s.ctx
	if task.Options != nil && task.Options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, task.Options.Timeout)
		defer cancel()
	}

	// Process the task
	err := handler(ctx, task)
	if err != nil {
		s.logError("Failed to process task", err)
		
		if task.Options != nil && task.Options.MaxRetries > 0 {
			msg.Nak() // Request redelivery
		} else {
			// Move to DLQ if enabled, otherwise terminate
			if s.config.DLQEnabled {
				if dlqErr := s.moveToDLQ(msg, task, err); dlqErr != nil {
					s.logError("Failed to move task to DLQ", dlqErr)
				}
			}
			msg.Term()
		}
		return
	}

	s.logError("Successfully processed message", fmt.Errorf("queue: %s, id: %s", taskMsg.Queue, taskMsg.ID))

	// Acknowledge successful processing
	msg.Ack()
}

func (s *Server) moveToDLQ(msg *nats.Msg, task *Task, processingErr error) error {
    // Create DLQ message with additional metadata
    dlqMsg := struct {
        Task           *Task   `json:"task"`
        OriginalQueue string  `json:"original_queue"`
        Error         string  `json:"error"`
        FailedAt      time.Time `json:"failed_at"`
        Retries       int    `json:"retries"`
    }{
        Task:           task,
        OriginalQueue: task.Queue,
        Error:         processingErr.Error(),
        FailedAt:      time.Now(),
        Retries:       task.Options.MaxRetries,
    }

    // Marshal DLQ message
    dlqData, err := json.Marshal(dlqMsg)
    if err != nil {
        return fmt.Errorf("failed to marshal DLQ message: %v", err)
    }

    // Get DLQ subject for the queue
    qConfig := s.getQueueConfig(task.Queue)
    dlqSubject := qConfig.DLQSubject
    if dlqSubject == "" {
        dlqSubject = fmt.Sprintf("%s.dlq.%s", s.streamName, task.Queue)
    }

    // Publish to DLQ
    _, err = s.js.Publish(dlqSubject, dlqData)
    if err != nil {
        return fmt.Errorf("failed to publish to DLQ: %v", err)
    }

    // Log the DLQ event
    s.logError(fmt.Sprintf("Task moved to DLQ: %s", task.Queue), processingErr)

    return nil
}

func (s *Server) getQueueConfig(queueName string) QueueConfig {
    for _, q := range s.config.Queues {
        if q.Name == queueName {
            return q
        }
    }
    return QueueConfig{}
}

func (s *Server) logError(msg string, err error) {
	if s.config.ErrorLogger != nil {
		s.config.ErrorLogger.Printf("%s: %v", msg, err)
	}
}

// Stop gracefully stops the server
func (s *Server) Stop() error {
	s.cancel()

	// Stop all worker pools
	for _, pool := range s.workers {
		pool.Stop()
	}

	// Unsubscribe from all subscriptions
	for _, sub := range s.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			s.logError("failed to unsubscribe", err)
		}
	}

	if s.nc != nil {
		s.nc.Close()
	}

	return nil
}

// WorkerPool manages a pool of workers
type WorkerPool struct {
	workers int
	tasks   chan func()
	quit    chan struct{}
	ctx     context.Context
}

func newWorkerPool(ctx context.Context, count int) *WorkerPool {
	p := &WorkerPool{
		workers: count,
		tasks:   make(chan func(), count*10), // Buffer the channel
		quit:    make(chan struct{}),
		ctx:     ctx,
	}

	for i := 0; i < count; i++ {
		go p.worker()
	}

	return p
}

func (p *WorkerPool) worker() {
	for {
		select {
		case task := <-p.tasks:
			select {
			case <-p.ctx.Done():
				return
			default:
				task()
			}
		case <-p.quit:
			return
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *WorkerPool) Submit(task func()) {
	select {
	case p.tasks <- task:
	case <-p.quit:
	case <-p.ctx.Done():
	default:
		// Channel is full, execute in current goroutine
		task()
	}
}

func (p *WorkerPool) Stop() {
	close(p.quit)
}
