package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"keyflicks_app/internals/s3_store"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// dataInRedis struct matches the JSON in your "VideoInfoOf:{id}" key
type dataInRedis struct {
	UserId      string `json:"user_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// DBWriter holds the service's dependencies and state
type DBWriter struct {
	db              *pgxpool.Pool
	redis           *redis.Client
	streamName      string
	groupName       string
	s3              *s3_store.S3Store
	streamingBucket string
	consumerName    string
	numWorkers      int
	jobsChannel     chan redis.XMessage
	wg              *sync.WaitGroup
}

// New creates a new DBWriter service
func NewDBWriter(db *pgxpool.Pool, redis_cli *redis.Client, numWorkers int) *DBWriter {
	// Generate a unique consumer name for this instance
	hostname, _ := os.Hostname()
	consumerName := fmt.Sprintf("dbwriter-%s-%d", hostname, os.Getpid())

	return &DBWriter{
		db:           db,
		redis:        redis_cli,
		streamName:   "Event.Transcode.Status", // The stream from your worker
		groupName:    "dbwriters",              // The name of your consumer group
		numWorkers:   numWorkers,
		consumerName: consumerName,
		jobsChannel:  make(chan redis.XMessage, numWorkers*2), // Buffered channel
		wg:           &sync.WaitGroup{},
	}
}

// Start launches the worker pool and the dispatcher
// Run this as a goroutine from your main.go
func (w *DBWriter) Start(ctx context.Context) {
	log.Println("Starting DB Writer service...")

	// 1. Ensure the stream and consumer group exist
	// This is idempotent. It creates the stream if it doesn't exist.
	err := w.redis.XGroupCreateMkStream(ctx, w.streamName, w.groupName, "0").Err()
	if err != nil && !errors.Is(err, redis.Nil) && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	// 2. Start the worker pool
	for i := 0; i < w.numWorkers; i++ {
		w.wg.Add(1)
		go w.worker(ctx, i+1)
	}

	// 3. Start the dispatcher to read from Redis
	go w.dispatcher(ctx)

	log.Printf("DB Writer service running with %d workers, consumer: %s", w.numWorkers, w.consumerName)

	// 4. Wait for context to be cancelled
	<-ctx.Done()
	log.Println("DB Writer: Shutdown signal received...")
	close(w.jobsChannel) // Signal workers to stop processing new jobs
	w.wg.Wait()          // Wait for all workers to finish
	log.Println("DB Writer service shut down gracefully.")
}

// dispatcher reads from XReadGroup and feeds the jobsChannel
func (w *DBWriter) dispatcher(ctx context.Context) {
	for {
		// Check for shutdown *before* blocking
		if ctx.Err() != nil {
			return // Context was cancelled
		}

		streams, err := w.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    w.groupName,
			Consumer: w.consumerName,
			Streams:  []string{w.streamName, ">"}, // ">" = new messages only
			Count:    int64(w.numWorkers * 2),     // Get a batch
			Block:    5 * time.Second,
			NoAck:    false, // We *must* ACK messages
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // Timeout, just loop again
			}
			if errors.Is(err, context.Canceled) {
				return // Shutting down
			}
			log.Printf("Dispatcher: Error reading from stream: %v", err)
			time.Sleep(1 * time.Second) // Backoff
			continue
		}

		// Push all received messages into the job channel
		for _, str := range streams {
			for _, msg := range str.Messages {
				select {
				case w.jobsChannel <- msg:
					// Job queued for a worker
				case <-ctx.Done():
					// Context cancelled, stop dispatching
					return
				}
			}
		}
	}
}

// worker processes jobs from the jobsChannel
func (w *DBWriter) worker(ctx context.Context, id int) {
	defer w.wg.Done()
	log.Printf("DB Writer Worker %d started", id)

	for msg := range w.jobsChannel {
		err := w.processJob(ctx, &msg)

		if err != nil {
			log.Printf("Worker %d: FAILED job %s: %v", id, msg.ID, err)
			// Don't ACK. Message stays pending for another consumer
			// or a recovery process (XClaim).
		} else {
			// 3. ACK after success
			if err := w.redis.XAck(ctx, w.streamName, w.groupName, msg.ID).Err(); err != nil {
				log.Printf("Worker %d: FAILED to ACK message %s: %v", id, msg.ID, err)
			} else {
				log.Printf("Worker %d: SUCCESS and ACK'd message %s", id, msg.ID)
			}
		}
	}
	log.Printf("DB Writer Worker %d shutting down", id)
}

// processJob is the actual business logic for your worker
func (w *DBWriter) processJob(ctx context.Context, msg *redis.XMessage) error {
	// 1. Decode msg.Values from "Event.Transcode.Status"
	uploadID, _ := msg.Values["upload_id"].(string)
	status, _ := msg.Values["status"].(string)

	if uploadID == "" || status == "" {
		return fmt.Errorf("invalid message format: missing upload_id or status")
	}

	// This is the stream your SSE handler is listening to
	sseStream := fmt.Sprintf("job_status:%s", uploadID)
	infoKey := fmt.Sprintf("VideoInfoOf:%s", uploadID)

	// 2. Handle the "failed" case first
	if status == "failed" {
		log.Printf("Processing FAILED job for %s", uploadID)

		// 2a. Publish to SSE stream
		w.redis.XAdd(ctx, &redis.XAddArgs{
			Stream: sseStream,
			Values: map[string]interface{}{"status": "failed"},
		})

		// 2b. Set expiry on SSE stream
		w.redis.Expire(ctx, sseStream, 2*24*time.Hour)

		// 2c. Clean up the info key
		w.redis.Del(ctx, infoKey)

		return nil // The "job" is done, even though it failed.
	}

	// 3. Handle the "ready" case
	if status == "ready" {
		// 3a. Get info from Redis
		infoJSON, err := w.redis.Get(ctx, infoKey).Result()
		if err != nil {
			// --- THIS IS YOUR NEW CLEANUP LOGIC ---
			log.Printf("CRITICAL: Job for %s is 'ready' but VideoInfo is missing from Redis. Deleting orphaned S3 files.", uploadID)

			// Use a new background context for cleanup to ensure it runs
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			//Delete from streaming bucket
			streamPrefix := fmt.Sprintf("videos/%s/", uploadID)
			if err := w.s3.DeleteObjectsByPrefix(cleanupCtx, w.streamingBucket, streamPrefix); err != nil {
				log.Printf("Cleanup FAILED for bucket %s, prefix %s: %v", w.streamingBucket, streamPrefix, err)
			}

			log.Printf("failed to get video info from redis (%s): %v", infoKey, err)

			// 2a. Publish to SSE stream
			w.redis.XAdd(ctx, &redis.XAddArgs{
				Stream: sseStream,
				Values: map[string]interface{}{"status": "failed"},
			})

			// 2b. Set expiry on SSE stream
			w.redis.Expire(ctx, sseStream, 2*24*time.Hour)

			return nil // The "job" is done, even though it failed.
			// --- END OF NEW LOGIC ---
		}

		// 3b. Unmarshal info
		var info dataInRedis
		if err := json.Unmarshal([]byte(infoJSON), &info); err != nil {
			return fmt.Errorf("failed to unmarshal video info JSON: %w", err)
		}

		// 3c. Write to Postgres (Idempotent Check)
		sql := `INSERT INTO videos (id, user_id, title, description)
                VALUES ($1, $2, $3, $4)`

		_, err = w.db.Exec(ctx, sql, uploadID, info.UserId, info.Title, info.Description)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
				log.Printf("Job %s already processed. Ignoring duplicate.", msg.ID)
				// This is OK. The job is idempotent.
			} else {
				return fmt.Errorf("failed to write to postgres: %w", err)
			}
		}

		// 3d. Publish to SSE stream
		w.redis.XAdd(ctx, &redis.XAddArgs{
			Stream: sseStream,
			Values: map[string]interface{}{"status": "ready"},
		})

		// 3e. Set expiry on SSE stream (2 days)
		w.redis.Expire(ctx, sseStream, 2*24*time.Hour)

		// 3f. Clean up the info key
		w.redis.Del(ctx, infoKey)

		return nil
	}

	// If status is "processing" or something else, we ignore it but still ACK
	log.Printf("Ignoring status '%s' for job %s", status, msg.ID)
	return nil
}
