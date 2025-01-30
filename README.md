# ZenTask

[![Go Reference](https://pkg.go.dev/badge/github.com/minasoft-technology/zentask.svg)](https://pkg.go.dev/github.com/minasoft-technology/zentask)
[![Go Report Card](https://goreportcard.com/badge/github.com/minasoft-technology/zentask)](https://goreportcard.com/report/github.com/minasoft-technology/zentask)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

ZenTask is a distributed task processing system built on top of NATS JetStream, providing a reliable and scalable solution for handling background jobs in Go applications. It offers a simple yet powerful API for distributed task processing with features like retries, scheduling, and dead letter queues.

## Features

- **Simple API**: Easy to use API for enqueueing and processing tasks
- **Distributed Processing**: Built on NATS JetStream for reliable message delivery
- **Flexible Task Options**: Support for delayed tasks, retries, and priorities
- **Automatic Retries**: Configurable retry policies with backoff
- **Dead Letter Queue**: Automatic handling of failed tasks
- **Worker Pools**: Configurable worker pools per queue
- **Graceful Shutdown**: Clean shutdown with task completion

## Installation

```bash
go get github.com/minasoft-technology/zentask
```

## Quick Start

### Server Example

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/minasoft-technology/zentask"
)

func main() {
    // Initialize server
    server, err := zentask.NewServer(zentask.ServerConfig{
        NatsURL: "nats://localhost:4222",
        StreamName: "ZENTASK",
        Queues: []zentask.QueueConfig{
            {
                Name:        "emails",
                WorkerCount: 2,
                MaxRetries:  3,
                RetryBackoff: time.Second,
                MaxAckPending: 100,
                AckWait:      time.Minute,
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer server.Stop()

    // Register handler
    err = server.RegisterHandler("emails", func(ctx context.Context, task *zentask.Task) error {
        email := task.Payload.(map[string]interface{})
        log.Printf("Sending email to: %v", email["to"])
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }

    // Start processing
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }

    // Wait for shutdown signal
    <-make(chan struct{})
}
```

### Client Example

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/minasoft-technology/zentask"
)

func main() {
    // Initialize client
    client, err := zentask.NewClient(zentask.Config{
        NatsURL:    "nats://localhost:4222",
        StreamName: "ZENTASK",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Create and enqueue a task
    task := &zentask.Task{
        Queue: "emails",
        Payload: map[string]interface{}{
            "to":      "user@example.com",
            "subject": "Welcome!",
            "body":    "Welcome to our service!",
        },
        Options: &zentask.TaskOptions{
            MaxRetries:  3,
            RetryDelay:  time.Second * 5,
            Priority:    1,
            ProcessAt:   time.Now().Add(time.Minute),
        },
    }
    
    info, err := client.Enqueue(task)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Task enqueued with ID: %s", info.ID)
}
```

## Configuration

### Client Configuration

```go
type Config struct {
    NatsURL       string        // NATS server URL
    StreamName    string        // JetStream stream name
    RetryAttempts int          // Number of retry attempts for NATS operations
    RetryBackoff  time.Duration // Backoff duration between retries
    RateLimit     float64      // Rate limit for task submission
    RateBurst     int          // Burst size for rate limiting
}
```

### Server Configuration

```go
type ServerConfig struct {
    NatsURL        string         // NATS server URL
    StreamName     string         // JetStream stream name
    Queues         []QueueConfig  // Queue configurations
    ErrorLogger    *log.Logger    // Error logger
    StreamReplicas int           // Number of stream replicas
    StreamMaxAge   time.Duration  // Maximum age of stream messages
    DLQEnabled     bool          // Enable Dead Letter Queue
    DLQRetention   time.Duration  // DLQ message retention period
}

type QueueConfig struct {
    Name           string        // Queue name
    WorkerCount    int          // Number of concurrent workers
    MaxRetries     int          // Maximum retry attempts
    RetryBackoff   time.Duration // Backoff duration between retries
    MaxAckPending  int          // Maximum pending acknowledgments
    AckWait        time.Duration // Acknowledgment wait timeout
    DLQSubject     string       // Dead Letter Queue subject
}
```

## Task Options

```go
type TaskOptions struct {
    MaxRetries  int           // Maximum retry attempts
    RetryDelay  time.Duration // Delay between retries
    Timeout     time.Duration // Task execution timeout
    Priority    int          // Task priority (higher = more priority)
    ProcessAt   time.Time    // Scheduled processing time
    UniqueKey   string       // Unique key for deduplication
    GroupID     string       // Group ID for batch processing
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
