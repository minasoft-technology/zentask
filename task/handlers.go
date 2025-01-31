package task

// RegisterDefaultHandlers registers all default task handlers with the processor
func RegisterDefaultHandlers(p *Processor) error {
	// Register all your handlers here
	handlers := map[string]Handler{
		// Example:
		// "email": NewEmailHandler(),
		// "notification": NewNotificationHandler(),
		// Add all your handlers here
	}

	for taskType, handler := range handlers {
		if err := p.RegisterHandler(taskType, handler); err != nil {
			return err
		}
	}

	return nil
}
