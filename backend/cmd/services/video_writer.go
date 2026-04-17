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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// dataInRedis struct matches the JSON in your "VideoInfoOf:{id}" key
type dataInRedis struct {
	UserId      string `json:"user_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

const (
	MaxBatchSize = 50              // Maximum number of videos to process in one transaction
	BlockTimeout = 2 * time.Second // Maximum time a worker waits to fill a batch
)

type DBWriter struct {
	db         *pgxpool.Pool
	redis      *redis.Client
	streamName string
	groupName  string
	numWorkers int
	wg         *sync.WaitGroup
}

// NewDBWriter creates a new DBWriter service
func NewDBWriter(db *pgxpool.Pool, redisCli *redis.Client, numWorkers int) *DBWriter {
	return &DBWriter{
		db:         db,
		redis:      redisCli,
		streamName: "Event.Transcode.Status",
		groupName:  "dbwriters",
		numWorkers: numWorkers,
		wg:         &sync.WaitGroup{},
	}
}

// Start initializes the consumer group and launches the independent worker pool
func (w *DBWriter) Start(ctx context.Context) {
	log.Println("Starting DB Writer Batch service...")

	// 1. Ensure the stream and consumer group exist idempotently
	err := w.redis.XGroupCreateMkStream(ctx, w.streamName, w.groupName, "0").Err()
	if err != nil && !errors.Is(err, redis.Nil) && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	hostname, _ := os.Hostname()

	// 2. Launch the independent worker pool
	for i := 0; i < w.numWorkers; i++ {
		w.wg.Add(1)
		// Give each worker a strictly unique consumer name (e.g., dbwriter-host-pid-worker1)
		// This ensures Redis balances the load perfectly across all goroutines
		consumerName := fmt.Sprintf("dbwriter-%s-%d-worker%d", hostname, os.Getpid(), i+1)
		go w.worker(ctx, i+1, consumerName)
	}

	log.Printf("DB Writer Batch service running with %d workers", w.numWorkers)

	// 3. Graceful Shutdown Listener
	<-ctx.Done()
	log.Println("DB Writer: Shutdown signal received. Waiting for active batches to finish...")
	w.wg.Wait()
	log.Println("DB Writer service shut down gracefully.")
}

// worker is an independent consumer that constantly pulls and processes batches
func (w *DBWriter) worker(ctx context.Context, workerID int, consumerName string) {
	defer w.wg.Done()
	log.Printf("Video writer worker %d (%s) started", workerID, consumerName)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down...", workerID)
			return
		default:
			// Fetch a BATCH of messages directly from Redis
			streams, err := w.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    w.groupName,
				Consumer: consumerName,
				Streams:  []string{w.streamName, ">"},
				Count:    MaxBatchSize, // From your constants (e.g., 50)
				Block:    BlockTimeout, // From your constants (e.g., 2 * time.Second)
				NoAck:    false,
			}).Result()

			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue // Normal timeout, no new messages. Loop again.
				}
				if errors.Is(err, context.Canceled) {
					// Context was canceled while blocking in XReadGroup
					return
				}
				log.Printf("Worker %d: Redis read error: %v", workerID, err)
				time.Sleep(1 * time.Second) // Backoff on actual connection errors
				continue
			}

			// Extract the messages from the stream response
			if len(streams) > 0 && len(streams[0].Messages) > 0 {
				batch := streams[0].Messages

				// Process the batch and get back only the successful IDs
				successfulIDs := w.processBatch(ctx, batch, workerID)

				// Bulk ACK the successful messages in one command
				if len(successfulIDs) > 0 {
					if err := w.redis.XAck(ctx, w.streamName, w.groupName, successfulIDs...).Err(); err != nil {
						log.Printf("Worker %d: CRITICAL - Failed to ACK successful batch: %v", workerID, err)
					} else {
						log.Printf("Worker %d: Successfully processed and ACK'd %d videos", workerID, len(successfulIDs))
					}
				}
			}
		}
	}
}

func (w *DBWriter) processBatch(ctx context.Context, batch []redis.XMessage, workerID int) []string {
	var successfulMsgIDs []string

	var readyUploads []struct{ MsgID, UploadID string }
	var failedUploads []struct{ MsgID, UploadID string }

	// --- 1. SEPARATE BATCH INTO "READY" AND "FAILED" ---
	for _, msg := range batch {
		uploadID, ok1 := msg.Values["upload_id"].(string)
		status, ok2 := msg.Values["status"].(string)

		// Safety check: if message is malformed, ACK it so it gets dropped
		if !ok1 || !ok2 {
			log.Printf("Worker %d: Malformed message %s, skipping", workerID, msg.ID)
			successfulMsgIDs = append(successfulMsgIDs, msg.ID)
			continue
		}

		if status == "ready" {
			readyUploads = append(readyUploads, struct{ MsgID, UploadID string }{msg.ID, uploadID})
		} else if status == "failed" {
			failedUploads = append(failedUploads, struct{ MsgID, UploadID string }{msg.ID, uploadID})
		} else {
			// Unknown status, ACK and drop
			successfulMsgIDs = append(successfulMsgIDs, msg.ID)
		}
	}

	// --- 2. FAST METADATA FETCH (REDIS MGET) ---
	var idsToInsert, userIDs, titles, descriptions []string
	readyMap := make(map[string]string) // Maps UploadID -> Redis MsgID for ACKing

	if len(readyUploads) > 0 {
		mgetKeys := make([]string, len(readyUploads))
		for i, u := range readyUploads {
			mgetKeys[i] = fmt.Sprintf("VideoInfoOf:%s", u.UploadID)
			readyMap[u.UploadID] = u.MsgID
		}

		// Fetch all metadata for all videos in a single network hop
		metaVals, err := w.redis.MGet(ctx, mgetKeys...).Result()
		if err != nil {
			log.Printf("Worker %d: Failed to MGET metadata: %v", workerID, err)
			return successfulMsgIDs // DB is untouched, return early. Messages remain un-ACKed to retry later.
		}

		for i, val := range metaVals {
			if val == nil {
				log.Printf("Worker %d: Metadata missing for %s. Cannot insert.", workerID, readyUploads[i].UploadID)
				successfulMsgIDs = append(successfulMsgIDs, readyUploads[i].MsgID) // ACK to drop
				continue
			}

			var data dataInRedis
			if err := json.Unmarshal([]byte(val.(string)), &data); err != nil {
				log.Printf("Worker %d: Invalid JSON for %s", workerID, readyUploads[i].UploadID)
				successfulMsgIDs = append(successfulMsgIDs, readyUploads[i].MsgID) // ACK to drop
				continue
			}

			idsToInsert = append(idsToInsert, readyUploads[i].UploadID)
			userIDs = append(userIDs, data.UserId)
			titles = append(titles, data.Title)
			descriptions = append(descriptions, data.Description)
		}
	}

	// --- 3. BULK DATABASE INSERTION (POSTGRESQL UNNEST) ---
	var successfullyInsertedUploadIDs []string
	var uniqueUserIDsForCache = make(map[string]bool)

	if len(idsToInsert) > 0 {
		query := `
			INSERT INTO videos (id, user_id, title, description)
			SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::text[])
			ON CONFLICT (id) DO NOTHING
			RETURNING id, user_id;
		`

		rows, err := w.db.Query(ctx, query, idsToInsert, userIDs, titles, descriptions)
		if err != nil {
			log.Printf("Worker %d: Bulk DB insert failed: %v", workerID, err)
			// Return immediately. We DO NOT append to successfulMsgIDs, so Redis will
			// keep them in the Pending queue and redeliver them to another worker.
			return successfulMsgIDs
		}
		defer rows.Close()

		for rows.Next() {
			var insertedID, insertedUserID string
			if err := rows.Scan(&insertedID, &insertedUserID); err == nil {
				successfullyInsertedUploadIDs = append(successfullyInsertedUploadIDs, insertedID)
				uniqueUserIDsForCache[insertedUserID] = true

				// Mark this specific Redis message as ready to ACK
				if msgID, exists := readyMap[insertedID]; exists {
					successfulMsgIDs = append(successfulMsgIDs, msgID)
				}
			}
		}

		// Handle duplicates (ON CONFLICT DO NOTHING means they won't return in RETURNING)
		// We still need to ACK duplicates so they don't get stuck forever.
		insertedCheck := make(map[string]bool)
		for _, id := range successfullyInsertedUploadIDs {
			insertedCheck[id] = true
		}
		for _, id := range idsToInsert {
			if !insertedCheck[id] {
				// It was a duplicate. Mark for ACK and SSE processing.
				if msgID, exists := readyMap[id]; exists {
					successfulMsgIDs = append(successfulMsgIDs, msgID)
					successfullyInsertedUploadIDs = append(successfullyInsertedUploadIDs, id)
				}
			}
		}
	}

	// --- 4. THE REDIS PIPELINE (SSE, CLEANUP, CACHE INVALIDATION) ---
	// We bundle all Redis commands for the entire batch into a single network request.
	pipe := w.redis.Pipeline()

	// A. Process "Ready" Videos
	for _, uploadID := range successfullyInsertedUploadIDs {
		sseStream := fmt.Sprintf("job_status:%s", uploadID)

		// 1. Push SSE Status
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: sseStream,
			Values: map[string]any{"status": "ready"},
		})
		// 2. Set 2-Day Safety Expiry
		pipe.Expire(ctx, sseStream, 48*time.Hour)
		// 3. Delete temporary metadata
		pipe.Del(ctx, fmt.Sprintf("VideoInfoOf:%s", uploadID))
	}

	// B. Cache Invalidation
	for uID := range uniqueUserIDsForCache {
		// Immediately clear the user's first page cache so their dashboard updates instantly
		pipe.Del(ctx, fmt.Sprintf("user_videos:%s:first_page", uID))

		// (Optional) If you want to delete ALL pages for this user efficiently in the future,
		// you can run an EVAL Lua script here:
		// pipe.Eval(ctx, "for _,k in ipairs(redis.call('keys', ARGV[1])) do redis.call('del', k) end", []string{}, fmt.Sprintf("user_videos:%s:*", uID))
	}

	// C. Process "Failed" Videos
	for _, fail := range failedUploads {
		sseStream := fmt.Sprintf("job_status:%s", fail.UploadID)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream:     sseStream,
			NoMkStream: true,
			Values:     map[string]any{"status": "failed"},
		})
		pipe.Expire(ctx, sseStream, 48*time.Hour)
		pipe.Del(ctx, fmt.Sprintf("VideoInfoOf:%s", fail.UploadID))

		// Mark failed messages as successfully handled by the pipeline
		successfulMsgIDs = append(successfulMsgIDs, fail.MsgID)
	}

	// Execute the massive pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("Worker %d: Redis Pipeline execution failed: %v", workerID, err)
		// Even if the pipeline fails, the DB insert succeeded. Returning the successfulMsgIDs
		// is safe because we don't want to insert into PostgreSQL twice.
	}

	return successfulMsgIDs
}
