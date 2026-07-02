package job

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusRunning    Status = "running"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
	StatusDeadLetter Status = "dead_letter"
)

type Job struct {
	ID         string
	Type       string
	Payload    map[string]any
	Status     Status
	Priority   int
	Attempts   int
	MaxRetries int

	CreatedAt time.Time
	UpdatedAt time.Time

	LastError string
}

type Option func(*Job)

func WithPriority(p int) Option {
	return func(j *Job) { j.Priority = p }
}

func WithMaxRetries(n int) Option {
	return func(j *Job) { j.MaxRetries = n }
}

func NewJob(jobType string, payload map[string]any, opts ...Option) (*Job, error) {
	if jobType == "" {
		return nil, errors.New("job type cannot be empty")
	}

	if payload == nil {
		return nil, errors.New("payload cannot be nil")
	}

	now := time.Now().UTC()
	j := &Job{
		ID:         uuid.NewString(),
		Type:       jobType,
		Payload:    payload,
		Status:     StatusPending,
		Priority:   0,
		MaxRetries: 3,
		Attempts:   0,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	for _, opt := range opts {
		opt(j)
	}
	if j.MaxRetries < 0 {
		return nil, errors.New("max retries cannot be negative")
	}
	return j, nil
}
