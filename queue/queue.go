package queue

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
)

type Queue struct {
	db               *sql.DB
	notifyChannel    string // for LISTEN/NOTIFY
	connectionString string // required for pq.NewListener
	// other fields if needed
}

// ProcessFunc is a signature for the function that processes a task
type ProcessFunc func(ctx context.Context, item *QueueItem) error

func NewQueue(db *sql.DB, connectionString string, notifyChannel string) (*Queue, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	return &Queue{
		db:               db,
		notifyChannel:    notifyChannel,
		connectionString: connectionString,
	}, nil
}

// updateTaskStatus updates the status of a task in the queue table.
func (q *Queue) updateTaskStatus(ctx context.Context, id int64, status Status) error {
	_, err := q.db.ExecContext(ctx, "UPDATE queue SET status = $1 WHERE id = $2", status, id)
	return err
}

// ListenAndProcess starts listening for notifications on the specified channel
// and processes tasks when a notification is received.
func (q *Queue) ListenAndProcess(ctx context.Context, processFunc ProcessFunc) {
	ln := pq.NewListener(q.connectionString, 1*time.Second, 1*time.Minute, func(event pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("PostgreSQL listener event error: %v", err)
		}
		switch event {
		case pq.ListenerEventConnected:
			log.Println("PostgreSQL listener connected")
		case pq.ListenerEventDisconnected:
			log.Println("PostgreSQL listener disconnected")
		case pq.ListenerEventReconnected:
			log.Println("PostgreSQL listener reconnected")
		}

		if err != nil {
			log.Printf("Error listening on channel: '%s' %v", q.notifyChannel, err)
		}

	})

	defer ln.Close()

	err := ln.Listen(q.notifyChannel)
	if err != nil {
		log.Printf("Error listening on channel: '%s' %v", q.notifyChannel, err)
	}

	log.Printf("Listening for notifications on channel: '%s'...", q.notifyChannel)

	for {
		select {
		case <-ctx.Done():
			log.Println("listener stopped")
			return
		case n := <-ln.Notify:
			if n != nil {
				log.Printf("Received notification on channel '%s', fetching pending task...", n.Channel)
				item, err := q.fetchNextPendingTask(context.Background()) // use the new Background context for fetching
				if err != nil {
					log.Printf("Error fetching pending task: %v", err)
					continue
				}
				if item != nil {
					log.Printf("Processing task with ID: %d", item.ID)
					err = processFunc(context.Background(), item) // use the new Background context for processing
					if err != nil {
						log.Printf("Error processing task ID %d: %v", item.ID, err)
						if err := q.updateTaskStatus(context.Background(), item.ID, Failed); err != nil {
							log.Printf("Error updating task status to %s for task ID %d: %v", Failed, item.ID, err)
						}
					} else {
						if err := q.updateTaskStatus(context.Background(), item.ID, Processed); err != nil {
							log.Printf("Error updating task status to %s for task ID %d: %v", Processed, item.ID, err)
						}
						log.Printf("Task ID %d processed successfully", item.ID)
					}
				} else {
					log.Println("No pending tasks found")
				}
			}
		}
	}
}

// Enqueue adds a new task to the queue and sends a notification.
func (q *Queue) Enqueue(ctx context.Context, payload interface{}) error {
	err := q.enqueueItem(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed to enqueue item: %w", err)
	}

	// Send a notification to the specific channel
	_, err = q.db.ExecContext(ctx, "NOTIFY "+q.notifyChannel)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	return nil
}
