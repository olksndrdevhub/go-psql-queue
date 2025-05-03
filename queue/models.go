package queue

import "time"

type Status string

const (
	Pending    Status = "pending"
	Processing Status = "processing"
	Processed  Status = "processed"
	Failed     Status = "failed"
	Success    Status = "success"
)

type QueueItem struct {
	ID              int64       `json:"id"`
	Payload         interface{} `json:"payload"`
	Status          string      `json:"status"`
	CreatedAt       time.Time   `json:"created_at"`
	Attempts        int         `json:"attempts"`
	LastAttemptedAt time.Time   `json:"last_attempted_at"`
}
