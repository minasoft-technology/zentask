package task

import (
	"encoding/json"
	"fmt"
)

// Payload represents the task payload data
type Payload struct {
	// Type identifies the kind of task
	Type string `json:"type"`
	// Data contains the actual task data
	Data json.RawMessage `json:"data"`
}

// NewPayload creates a new payload with the given type and data
func NewPayload(payloadType string, data interface{}) (*Payload, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload data: %w", err)
	}

	return &Payload{
		Type: payloadType,
		Data: dataBytes,
	}, nil
}

// UnmarshalData unmarshals the payload data into the given value
func (p *Payload) UnmarshalData(v interface{}) error {
	if err := json.Unmarshal(p.Data, v); err != nil {
		return fmt.Errorf("failed to unmarshal payload data: %w", err)
	}
	return nil
}

// Validate validates the payload
func (p *Payload) Validate() error {
	if p.Type == "" {
		return fmt.Errorf("payload type is required")
	}
	if len(p.Data) == 0 {
		return fmt.Errorf("payload data is required")
	}
	return nil
}
