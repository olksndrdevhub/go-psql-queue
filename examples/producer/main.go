package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/olksndrdevhub/go-psql-queue/queue"
)

func main() {
	connectionString := os.Getenv("DATABASE_URL")
	notifyChannel := os.Getenv("NOTIFY_CHANNEL")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer db.Close()

	// initialize the queue
	q, err := queue.NewQueue(db, connectionString, notifyChannel)
	if err != nil {
		log.Fatalf("Error initializing queue: %v", err)
	}

	// example payloads to enqueue
	tasks := []interface{}{
		map[string]string{"operation": "process_image", "image_id": "123"},
		map[string]interface{}{"operation": "square", "value": 123},
		"just simple message",
	}

	ctx := context.Background()

	for i, task := range tasks {
		log.Printf("Enqueuing task %d: %+v", i+1, task)
		err := q.Enqueue(ctx, task)
		if err != nil {
			log.Fatalf("Error enqueuing task %d: %v", i+1, err)
		}
		time.Sleep(1 * time.Second) // simulate some delay between enqueuing tasks
	}

	log.Println("All tasks enqueued successfully")
}
