## Getting Started

### Prerequisites

* Go (version 1.24 or later recommended)
* PostgreSQL database (version 9.5 or later recommended for `JSONB` and `SKIP LOCKED`)

### Setup

1.  **Clone the repository:**
    ```bash
    git clone [your_repository_url]
    cd queue_project
    ```

2.  **Initialize the Go module (if you haven't already):**
    ```bash
    go mod init [github.com/yourusername/queue_project](https://github.com/yourusername/queue_project) # Replace with your import path
    ```

3.  **Create the PostgreSQL queue table:**
    Connect to your PostgreSQL database and execute the following SQL:
    ```sql
    CREATE TABLE queue (
        id BIGSERIAL PRIMARY KEY,
        payload JSONB NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
        attempts INTEGER NOT NULL DEFAULT 0,
        last_attempted_at TIMESTAMP WITH TIME ZONE
    );

    CREATE INDEX idx_queue_status_created_at ON queue (status, created_at);
    ```

### Running the Examples

1.  **Configure Database Connection:**
    In both `examples/producer/main.go` and `examples/consumer/main.go`, update the `dbDSN` variable with your PostgreSQL connection details:
    ```go
    dbDSN := "user=youruser password=yourpassword host=localhost port=5432 dbname=yourdb sslmode=disable"
    ```

2.  **Run the Producer:**
    Open a terminal, navigate to the `examples/producer` directory, and run:
    ```bash
    go run main.go
    ```
    This will enqueue some example tasks into the PostgreSQL queue.

3.  **Run the Consumer:**
    Open another terminal, navigate to the `examples/consumer` directory, and run:
    ```bash
    go run main.go
    ```
    This will start the consumer, which will listen for notifications and process tasks from the queue. You should see output in the consumer's terminal as it processes the tasks enqueued by the producer.

## Using the `queue` Package in Your Own Project

1.  **Get the package:**
    ```bash
    go get [github.com/yourusername/queue_project/queue](https://github.com/yourusername/queue_project/queue) # Replace with your import path
    ```

2.  **Import the package in your Go code:**
    ```go
    import "[github.com/yourusername/queue_project/queue](https://github.com/yourusername/queue_project/queue)"
    ```

3.  **Establish a database connection:**
    You'll need to establish a `*sql.DB` connection to your PostgreSQL database.

4.  **Initialize the `Queue`:**
    ```go
    db, err := sql.Open("postgres", "your_db_connection_string")
    if err != nil {
        // handle error
    }
    defer db.Close()

    notifyChannel := "your_notification_channel_name"
    q, err := queue.NewQueue(db, "your_db_connection_string", notifyChannel)
    if err != nil {
        // handle error
    }
    ```

5.  **Enqueue tasks:**
    ```go
    payload := map[string]string{"action": "do_something", "data": "some data"}
    err = q.Enqueue(context.Background(), payload)
    if err != nil {
        // handle error
    }
    ```

6.  **Consume and process tasks:**
    You'll need to define a processing function that matches the `queue.ProcessFunc` signature and then call `q.ListenAndProcess`:
    ```go
    processTask := func(ctx context.Context, item *queue.QueueItem) error {
        log.Printf("Processing task ID: %d, Payload: %+v", item.ID, item.Payload)
        // Your processing logic here
        return nil // or an error if processing failed
    }

    ctx := context.Background()
    q.ListenAndProcess(ctx, processTask)
    ```

## Configuration

* **`notifyChannel`:** The name of the PostgreSQL notification channel used for signaling new tasks. Ensure the producer and consumer use the same channel name.
* **Database Connection String (DSN):** You'll need to provide the correct DSN for your PostgreSQL database when initializing the `Queue`.
* **Reconnect Intervals (Consumer):** The `ListenAndProcess` function in the consumer uses a minimum and maximum reconnect interval for the PostgreSQL listener (currently 1 second and 1 minute, respectively). You can adjust these within the `queue/queue.go` file if needed.

## Error Handling and Considerations

* The examples provide basic error logging. In a production environment, you'll need more robust error handling and potentially retry mechanisms.
* Consider how you want to handle task failures (e.g., updating the `status` to `failed`, implementing dead-letter queues, more sophisticated retry policies).
* For high-throughput systems, you might need to fine-tune your PostgreSQL configuration and consider partitioning the `queue` table.
* The `payload` is currently stored as `JSONB`, offering flexibility. Ensure your producer and consumer agree on the structure of the JSON data.

## Contributing

[Add your contributing guidelines here if you plan to open-source the project.]

## License

[Add your license information here.]
