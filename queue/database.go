package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// enqueueItem inserts a new item into the queue table.
func (q *Queue) enqueueItem(ctx context.Context, payload interface{}) error {
	query := `
    INSERT INTO queue (payload, created_at)
    VALUES ($1, $2)
    RETURNING id, created_at;
  `
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	var id int64
	var createdAt time.Time
	err = q.db.QueryRowContext(ctx, query, payloadJSON, time.Now()).Scan(&id, &createdAt)
	if err != nil {
		return err
	}

	return nil
}

// fetchNextPendingTask fetches the next pending task from the queue, locking it.
// the lock is released when the transaction is commited or rolled back.
// using FOR UPDATE SKIP LOCKED ensures that muptiplr workers can concurrently
// try to fetch tasks without blocking each other.
func (q *Queue) fetchNextPendingTask(ctx context.Context) (*QueueItem, error) {
	tx, err := q.db.BeginTx(ctx, nil) // start a transaction
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // rollback the transaction if not commited

	query := `
          SELECT id, payload, status, created_at, attempts, last_attempted_at
          FROM queue
          WHERE status = $1
          ORDER BY created_at
          LIMIT 1;
          FOR UPDATE SKIP LOCKED;
  `

	row := tx.QueryRowContext(ctx, query, Pending)

	var item QueueItem
	var payloadJSON []byte
	err = row.Scan(&item.ID, &payloadJSON, &item.Status, &item.CreatedAt, &item.Attempts, &item.LastAttemptedAt)
	if err == sql.ErrNoRows {
		return nil, nil // no pending tasks
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal(payloadJSON, &item.Payload)
	if err != nil {
		return nil, err
	}

	// update status to processing within the same transaction
	updateQuery := `
                UPDATE queue
                SET status = $1, last_attempted_at = $2, attempts = attempts + 1
                WHERE id = $3
  `
	_, err = tx.ExecContext(ctx, updateQuery, Processing, time.Now(), item.ID)
	if err != nil {
		return nil, err
	}

	return &item, tx.Commit() // commit the transaction if everything went well

}
