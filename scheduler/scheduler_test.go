package scheduler

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/minasoft-technology/zentask"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func TestScheduler_AddSchedule(t *testing.T) {
	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://nats_user:nats_password@localhost:4222",
	})
	if err != nil {
		t.Logf("NATS connection error: %v", err)
		t.Skip("Skipping test: NATS server not available")
		return
	}
	defer client.Close()

	scheduler := NewScheduler(client, WithSeconds(true))
	scheduler.Start()
	defer scheduler.Stop()

	tests := []struct {
		name      string
		schedule  *Schedule
		wantError bool
	}{
		{
			name: "valid 6-field cron expression",
			schedule: &Schedule{
				ID:          "test1",
				CronExpr:    "*/5 * * * * *", // Run every 5 seconds
				Queue:       "test",
				Description: "Test schedule with seconds",
				Payload: map[string]interface{}{
					"test": "data",
				},
			},
			wantError: false,
		},
		{
			name: "valid 5-field cron expression",
			schedule: &Schedule{
				ID:          "test2",
				CronExpr:    "*/5 * * * *", // Run every 5 minutes
				Queue:       "test",
				Description: "Test schedule without seconds",
				Payload: map[string]interface{}{
					"test": "data",
				},
			},
			wantError: false,
		},
		{
			name: "invalid cron expression",
			schedule: &Schedule{
				ID:          "test3",
				CronExpr:    "invalid",
				Queue:       "test",
				Description: "Invalid schedule",
			},
			wantError: true,
		},
		{
			name: "duplicate schedule ID",
			schedule: &Schedule{
				ID:          "test1", // Same ID as first test
				CronExpr:    "*/5 * * * * *",
				Queue:       "test",
				Description: "Duplicate ID test",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := scheduler.AddSchedule(tt.schedule)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Verify schedule was added
			saved, err := scheduler.GetSchedule(tt.schedule.ID)
			require.NoError(t, err)
			require.Equal(t, tt.schedule.CronExpr, saved.CronExpr)
			require.Equal(t, tt.schedule.Queue, saved.Queue)
			require.Equal(t, tt.schedule.Description, saved.Description)

			// Verify next run time was calculated
			require.False(t, saved.NextRun.IsZero())
			require.True(t, saved.NextRun.After(time.Now()))

			// Verify cronEntryID was set
			require.NotZero(t, saved.cronEntryID)
		})
	}
}

func TestScheduler_WithOptions(t *testing.T) {
	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://nats_user:nats_password@localhost:4222",
	})
	if err != nil {
		t.Logf("NATS connection error: %v", err)
		t.Skip("Skipping test: NATS server not available")
		return
	}
	defer client.Close()

	// Test with custom location
	t.Run("with custom location", func(t *testing.T) {
		loc, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)

		scheduler := NewScheduler(client, WithSeconds(true), WithLocation(loc))
		scheduler.Start()
		defer scheduler.Stop()

		schedule := &Schedule{
			ID:          "test-location",
			CronExpr:    "0 0 0 * * *", // Daily at midnight with seconds
			Queue:       "test",
			Description: "Test schedule with custom location",
		}

		err = scheduler.AddSchedule(schedule)
		require.NoError(t, err)

		saved, err := scheduler.GetSchedule(schedule.ID)
		require.NoError(t, err)

		// Verify next run is in NY timezone
		nyMidnight := time.Now().In(loc)
		nyMidnight = time.Date(nyMidnight.Year(), nyMidnight.Month(), nyMidnight.Day()+1, 0, 0, 0, 0, loc)
		require.Equal(t, nyMidnight.Unix(), saved.NextRun.Unix())
	})

	// Test with custom logger
	t.Run("with custom logger", func(t *testing.T) {
		logFile, err := os.CreateTemp("", "cron-test-*.log")
		require.NoError(t, err)
		defer os.Remove(logFile.Name())

		logger := cron.VerbosePrintfLogger(log.New(logFile, "cron: ", log.LstdFlags))
		scheduler := NewScheduler(client, WithSeconds(true), WithLogger(logger))
		scheduler.Start()
		defer scheduler.Stop()

		schedule := &Schedule{
			ID:          "test-logger",
			CronExpr:    "* * * * * *", // Run every second
			Queue:       "test",
			Description: "Test schedule with custom logger",
		}

		err = scheduler.AddSchedule(schedule)
		require.NoError(t, err)

		// Wait for some log entries
		time.Sleep(2 * time.Second)

		// Verify logs were written
		logContent, err := os.ReadFile(logFile.Name())
		require.NoError(t, err)
		require.NotEmpty(t, logContent)
	})
}

func TestScheduler_RemoveSchedule(t *testing.T) {
	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://nats_user:nats_password@localhost:4222",
	})
	if err != nil {
		t.Logf("NATS connection error: %v", err)
		t.Skip("Skipping test: NATS server not available")
		return
	}
	defer client.Close()

	scheduler := NewScheduler(client, WithSeconds(true))
	scheduler.Start()
	defer scheduler.Stop()

	// Add a schedule
	schedule := &Schedule{
		ID:          "test-remove",
		CronExpr:    "*/5 * * * * *",
		Queue:       "test",
		Description: "Test schedule for removal",
	}

	err = scheduler.AddSchedule(schedule)
	require.NoError(t, err)

	// Verify it was added
	saved, err := scheduler.GetSchedule(schedule.ID)
	require.NoError(t, err)
	originalEntryID := saved.cronEntryID

	// Remove the schedule
	err = scheduler.RemoveSchedule(schedule.ID)
	require.NoError(t, err)

	// Verify it was removed
	_, err = scheduler.GetSchedule(schedule.ID)
	require.Error(t, err)

	// Try to remove non-existent schedule
	err = scheduler.RemoveSchedule("non-existent")
	require.Error(t, err)

	// Verify the cron entry was removed by trying to add a new schedule with same expression
	newSchedule := &Schedule{
		ID:          "test-remove-2",
		CronExpr:    schedule.CronExpr,
		Queue:       "test",
		Description: "New test schedule",
	}

	err = scheduler.AddSchedule(newSchedule)
	require.NoError(t, err)

	// Verify new schedule got a different entry ID
	saved, err = scheduler.GetSchedule(newSchedule.ID)
	require.NoError(t, err)
	require.NotEqual(t, originalEntryID, saved.cronEntryID)
}

func TestScheduler_ListSchedules(t *testing.T) {
	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://nats_user:nats_password@localhost:4222",
	})
	if err != nil {
		t.Logf("NATS connection error: %v", err)
		t.Skip("Skipping test: NATS server not available")
		return
	}
	defer client.Close()

	scheduler := NewScheduler(client, WithSeconds(true))
	scheduler.Start()
	defer scheduler.Stop()

	// Add multiple schedules
	schedules := []*Schedule{
		{
			ID:          "test1",
			CronExpr:    "*/5 * * * * *",
			Queue:       "test",
			Description: "Test schedule 1",
		},
		{
			ID:          "test2",
			CronExpr:    "0 */5 * * * *",
			Queue:       "test",
			Description: "Test schedule 2",
		},
	}

	for _, s := range schedules {
		err := scheduler.AddSchedule(s)
		require.NoError(t, err)
	}

	// List all schedules
	list := scheduler.ListSchedules()
	require.Len(t, list, len(schedules))

	// Verify each schedule is in the list
	for _, s := range schedules {
		found := false
		for _, ls := range list {
			if ls.ID == s.ID {
				found = true
				require.Equal(t, s.CronExpr, ls.CronExpr)
				require.Equal(t, s.Queue, ls.Queue)
				require.Equal(t, s.Description, ls.Description)
				require.NotZero(t, ls.cronEntryID)
				break
			}
		}
		require.True(t, found, "Schedule %s not found in list", s.ID)
	}
}

func TestScheduler_Stop(t *testing.T) {
	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://nats_user:nats_password@localhost:4222",
	})
	if err != nil {
		t.Logf("NATS connection error: %v", err)
		t.Skip("Skipping test: NATS server not available")
		return
	}
	defer client.Close()

	scheduler := NewScheduler(client, WithSeconds(true))

	// Add a schedule
	schedule := &Schedule{
		ID:          "test-stop",
		CronExpr:    "*/1 * * * * *", // Run every second
		Queue:       "test",
		Description: "Test schedule for stop",
	}

	err = scheduler.AddSchedule(schedule)
	require.NoError(t, err)

	// Start the scheduler
	scheduler.Start()

	// Let it run for a moment
	time.Sleep(time.Second * 2)

	// Stop the scheduler and verify it completes
	doneCh := make(chan struct{})
	go func() {
		scheduler.Stop()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// Success - scheduler stopped
	case <-time.After(time.Second * 5):
		t.Fatal("Scheduler.Stop() did not complete within timeout")
	}
}
