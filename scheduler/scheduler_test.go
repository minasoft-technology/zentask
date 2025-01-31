package scheduler

import (
	"testing"
	"time"

	"github.com/minasoft-technology/zentask"
	"github.com/stretchr/testify/require"
)

func TestScheduler_AddSchedule(t *testing.T) {
	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://localhost:4222",
	})
	if err != nil {
		t.Skip("Skipping test: NATS server not available")
		return
	}
	defer client.Close()

	scheduler := NewScheduler(client)
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
		})
	}
}

func TestScheduler_RemoveSchedule(t *testing.T) {
	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://localhost:4222",
	})
	if err != nil {
		t.Skip("Skipping test: NATS server not available")
		return
	}
	defer client.Close()

	scheduler := NewScheduler(client)
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
	_, err = scheduler.GetSchedule(schedule.ID)
	require.NoError(t, err)

	// Remove the schedule
	err = scheduler.RemoveSchedule(schedule.ID)
	require.NoError(t, err)

	// Verify it was removed
	_, err = scheduler.GetSchedule(schedule.ID)
	require.Error(t, err)
}

func TestScheduler_ListSchedules(t *testing.T) {
	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://localhost:4222",
	})
	if err != nil {
		t.Skip("Skipping test: NATS server not available")
		return
	}
	defer client.Close()

	scheduler := NewScheduler(client)
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
				break
			}
		}
		require.True(t, found, "Schedule %s not found in list", s.ID)
	}
}
