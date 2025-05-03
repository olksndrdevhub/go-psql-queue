package main

import (
	"context"
	"database/sql"
	"fmt"
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

	// define the task processing function
	processTask := func(ctx context.Context, item *queue.QueueItem) error {
		log.Printf("Consumer received task ID: %d, Payload: %+v", item.ID, item.Payload)
		time.Sleep(3 * time.Second) // simulate processing time

		// validate patload and process
		payloadMap, ok := item.Payload.(map[string]interface{})
		if ok {
			if op, exists := payloadMap["operation"]; exists && op == "process_image" {
				log.Printf("processing image: %s", payloadMap["image_id"])
			} else if calc, exists := payloadMap["calculate"]; exists && calc == "square" {
				if val, ok := payloadMap["value"].(float64); ok {
					result := val * val
					log.Printf("calculated square of %f is %f", val, result)
				}
			}

		} else if strPayload, ok := item.Payload.(string); ok {
			log.Printf("processing simple message: %s", strPayload)
		}

		// simulate potential errorrs (for demo)
		if item.ID%3 == 0 {
			return fmt.Errorf("failed to process task ID %d", item.ID)
		}

		log.Printf("Consumer processed task ID: %d", item.ID)

		return nil // indicate successful processing
	}

	// start listening for and processing tasks
	ctx := context.Background()
	q.ListenAndProcess(ctx, processTask)

	// The ListenAndProcess function will run indefinitely until the context is cancelled.
	// In a real application, you might want to handle graceful shutdown using signals.
	select {} // Keep the main function running to allow the listener to work
}
