package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/minasoft-technology/zentask"
	"github.com/minasoft-technology/zentask/scheduler"
)

// ReportTask represents a periodic report generation task
type ReportTask struct {
	ReportType string    `json:"report_type"`
	StartDate  time.Time `json:"start_date"`
	EndDate    time.Time `json:"end_date"`
}

func main() {
	// Create ZenTask server
	server, err := zentask.NewServer(zentask.ServerConfig{
		NatsURL: "nats://localhost:4222",
		Queues: []zentask.QueueConfig{
			{
				Name:         "reports",
				WorkerCount:  2,
				MaxRetries:   3,
				RetryBackoff: time.Second,
			},
		},
		ErrorLogger: log.New(os.Stderr, "[ZENTASK] ", log.LstdFlags),
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Register report handler
	err = server.RegisterHandler("reports", func(ctx context.Context, task *zentask.Task) error {
		var report ReportTask
		payloadBytes, err := json.Marshal(task.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal task payload: %v", err)
		}
		if err := json.Unmarshal(payloadBytes, &report); err != nil {
			return fmt.Errorf("failed to unmarshal report task: %v", err)
		}

		log.Printf("Generating %s report for period: %v to %v",
			report.ReportType, report.StartDate, report.EndDate)

		// Simulate report generation
		time.Sleep(2 * time.Second)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to register handler: %v", err)
	}

	// Start the server
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Create ZenTask client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://localhost:4222",
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create scheduler
	taskScheduler := scheduler.NewScheduler(client)

	// Add daily sales report schedule (runs at midnight every day)
	dailySalesReport := &scheduler.Schedule{
		ID:          "daily-sales-report",
		CronExpr:    "0 0 * * *", // At 00:00 every day
		Queue:       "reports",
		Description: "Generate daily sales report",
		Payload: ReportTask{
			ReportType: "sales",
			// StartDate and EndDate will be set in the cron job
		},
		Options: &zentask.TaskOptions{
			MaxRetries: 3,
			RetryDelay: time.Minute,
			Priority:   1,
		},
		Metadata: map[string]interface{}{
			"department": "sales",
			"type":       "daily-report",
		},
	}

	if err := taskScheduler.AddSchedule(dailySalesReport); err != nil {
		log.Fatalf("Failed to add daily sales report schedule: %v", err)
	}

	// Add weekly inventory report schedule (runs at 1 AM every Monday)
	weeklyInventoryReport := &scheduler.Schedule{
		ID:          "weekly-inventory-report",
		CronExpr:    "0 1 * * 1", // At 01:00 on Monday
		Queue:       "reports",
		Description: "Generate weekly inventory report",
		Payload: ReportTask{
			ReportType: "inventory",
			// StartDate and EndDate will be set in the cron job
		},
		Options: &zentask.TaskOptions{
			MaxRetries: 3,
			RetryDelay: time.Minute,
			Priority:   2,
		},
		Metadata: map[string]interface{}{
			"department": "inventory",
			"type":       "weekly-report",
		},
	}

	if err := taskScheduler.AddSchedule(weeklyInventoryReport); err != nil {
		log.Fatalf("Failed to add weekly inventory report schedule: %v", err)
	}

	// Start the scheduler
	taskScheduler.Start()

	// Print all schedules
	fmt.Println("\nConfigured Schedules:")
	for _, schedule := range taskScheduler.ListSchedules() {
		fmt.Printf("ID: %s\nDescription: %s\nCron: %s\nNext Run: %v\n\n",
			schedule.ID, schedule.Description, schedule.CronExpr, schedule.NextRun)
	}

	// Keep the program running
	select {}
}
