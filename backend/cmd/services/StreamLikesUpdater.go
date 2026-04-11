package services

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"time"

	"keyflicks_app/internals/cache"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// LikeEvent represents a single like/unlike action parsed from the Redis Stream
type LikeEvent struct {
	MessageID string // We need this to ACK the message in Redis later
	VideoID   string
	UserID    string
	Action    string // "like" or "unlike"
}

type StreamLikesWorker struct {
	db    *pgxpool.Pool
	redis *cache.RedisDB
}

func NewStreamLikesWorker(db *pgxpool.Pool, redisCli *cache.RedisDB) *StreamLikesWorker {
	return &StreamLikesWorker{
		db:    db,
		redis: redisCli,
	}
}

// hashVideoID takes a UUID string and returns a deterministic integer between 0 and numWorkers-1
func (w *StreamLikesWorker) hashVideoID(videoID string, numWorkers int) int {
	h := fnv.New32a()
	h.Write([]byte(videoID))
	hashValue := int(h.Sum32())

	// Ensure the hash is positive before performing modulo
	if hashValue < 0 {
		hashValue = -hashValue
	}

	return hashValue % numWorkers
}

// Start initializes the worker channels and kicks off the stream consumer
func (w *StreamLikesWorker) Start(ctx context.Context, numWorkers int) {
	log.Printf("Starting StreamLikesWorker with %d partitioned workers...", numWorkers)

	// 1. Create a slice of buffered channels.
	// We use a buffer of 1000 so the central router doesn't block if a worker is busy writing to the DB.
	workerChannels := make([]chan LikeEvent, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = make(chan LikeEvent, 1000)

		// Start the individual worker goroutine
		go w.workerLoop(ctx, i, workerChannels[i])
	}

	// 2. Start the central stream consumer that routes events to the workers
	go w.consumeStream(ctx, numWorkers, workerChannels)

	<-ctx.Done()
	log.Println("StreamLikesWorker shutting down gracefully...")
}

// consumeStream is the central router. It reads from Redis and partitions data by VideoID.
func (w *StreamLikesWorker) consumeStream(ctx context.Context, numWorkers int, channels []chan LikeEvent) {
	streamKey := "stream:likes_ingest"
	groupName := "likes_worker_group"
	consumerName := "central_router"

	// Ensure the consumer group exists. Ignore error if it already exists (BUSYGROUP)
	err := w.redis.Client.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Block for up to 2 seconds waiting for new messages
			streams, err := w.redis.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamKey, ">"}, // ">" means give us messages never delivered to other consumers
				Count:    1000,                     // Pull up to 1000 events at a time
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					continue // Stream empty, blocked timeout reached, loop again
				}
				log.Printf("Error reading from stream: %v", err)
				time.Sleep(1 * time.Second) // Backoff on error
				continue
			}

			// Route each message to the correct worker
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					vID, ok1 := msg.Values["video_id"].(string)
					uID, ok2 := msg.Values["user_id"].(string)
					action, ok3 := msg.Values["action"].(string)

					if !ok1 || !ok2 || !ok3 {
						log.Printf("Malformed message in stream %s, skipping...", msg.ID)
						w.redis.Client.XAck(ctx, streamKey, groupName, msg.ID)
						continue
					}

					event := LikeEvent{
						MessageID: msg.ID,
						VideoID:   vID,
						UserID:    uID,
						Action:    action,
					}

					// THE MAGIC: Route to a specific worker channel based on a hash of the VideoID
					workerIndex := w.hashVideoID(vID, numWorkers)
					channels[workerIndex] <- event
				}
			}
		}
	}
}

// workerLoop listens on its specific channel, batches events, and flushes them to the DB.
// Because of the routing hash, Worker X is the ONLY worker that will ever see events for Video Y.
func (w *StreamLikesWorker) workerLoop(ctx context.Context, workerID int, ch <-chan LikeEvent) {
	const maxBatchSize = 500
	batch := make([]LikeEvent, 0, maxBatchSize)

	log.Printf("StreamLikesUpdater worker - %d has been started", workerID)

	// Flush whatever we have every 2 seconds, even if we haven't hit maxBatchSize
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case event := <-ch:
			batch = append(batch, event)

			// If our batch is full, process it immediately
			if len(batch) >= maxBatchSize {
				w.processBatch(ctx, workerID, batch)

				// Reset batch slice (reusing memory capacity)
				batch = make([]LikeEvent, 0, maxBatchSize)
				ticker.Reset(2 * time.Second) // Reset the ticker so we don't double-flush
			}

		case <-ticker.C:
			// The timer ticked. If we have anything in the batch, process it.
			if len(batch) > 0 {
				w.processBatch(ctx, workerID, batch)
				batch = make([]LikeEvent, 0, maxBatchSize)
			}
		}
	}
}

// processBatch takes the collapsed batch and safely updates the database and Redis counters.
// To be implemented in the next step!
func (w *StreamLikesWorker) processBatch(ctx context.Context, workerID int, batch []LikeEvent) {
	if len(batch) == 0 {
		return
	}

	// 1. IN-MEMORY SPAM COLLAPSING
	collapsedState := make(map[string]string)
	messageIDs := make([]string, 0, len(batch))

	for _, event := range batch {
		key := fmt.Sprintf("%s:%s", event.VideoID, event.UserID)
		collapsedState[key] = event.Action
		messageIDs = append(messageIDs, event.MessageID)
	}

	var insertUsers, insertVideos []string
	var deleteUsers, deleteVideos []string

	for key, action := range collapsedState {

		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			log.Printf("Worker %d: Malformed collapsed state key: %s", workerID, key)
			continue
		}

		vID := parts[0]
		uID := parts[1]

		if action == "like" {
			insertVideos = append(insertVideos, vID)
			insertUsers = append(insertUsers, uID)
		} else if action == "unlike" {
			deleteVideos = append(deleteVideos, vID)
			deleteUsers = append(deleteUsers, uID)
		}
	}

	videoDeltas := make(map[string]int64)

	// START TRANSACTION
	tx, err := w.db.Begin(ctx)
	if err != nil {
		log.Printf("Worker %d: Failed to begin transaction: %v", workerID, err)
		return
	}

	// defer rollback. If tx.Commit() succeeds, this does nothing.
	// If the function panics or returns early, this safely undoes partial work.
	defer tx.Rollback(ctx)

	// 2. BULK DATABASE INSERT (Validated "Likes")
	if len(insertVideos) > 0 {
		insertQuery := `
			INSERT INTO video_likes (user_id, video_id, type)
			SELECT unnested.user_id, unnested.video_id, 'like'
			FROM UNNEST($1::uuid[], $2::uuid[]) AS unnested(user_id, video_id)
			ON CONFLICT (user_id, video_id) DO NOTHING
			RETURNING video_id;
		`

		rows, err := tx.Query(ctx, insertQuery, insertUsers, insertVideos) // Note: using tx.Query
		if err != nil {
			log.Printf("Worker %d: Bulk insert failed: %v", workerID, err)
			return
		}

		for rows.Next() {
			var validVid string
			if err := rows.Scan(&validVid); err == nil {
				videoDeltas[validVid]++
			}
		}
		rows.Close() // Close early before next query

		if err := rows.Err(); err != nil {
			log.Printf("Worker %d: Bulk insert rows iteration failed: %v", workerID, err)
			return
		}
	}

	// 3. BULK DATABASE DELETE (Validated "Unlikes")
	if len(deleteVideos) > 0 {
		deleteQuery := `
			DELETE FROM video_likes vl
			USING UNNEST($1::uuid[], $2::uuid[]) AS unnested(user_id, video_id)
			WHERE vl.user_id = unnested.user_id AND vl.video_id = unnested.video_id
			RETURNING vl.video_id;
		`

		rows, err := tx.Query(ctx, deleteQuery, deleteUsers, deleteVideos) // Note: using tx.Query
		if err != nil {
			log.Printf("Worker %d: Bulk delete failed: %v", workerID, err)
			return
		}

		for rows.Next() {
			var validVid string
			if err := rows.Scan(&validVid); err == nil {
				videoDeltas[validVid]--
			}
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			log.Printf("Worker %d: Bulk delete rows iteration failed: %v", workerID, err)
			return
		}
	}

	// 4. BULK COUNTER UPDATES (Postgres)
	var updateIDs []string
	var updateCounts []int64

	for vid, delta := range videoDeltas {
		if delta != 0 {
			updateIDs = append(updateIDs, vid)
			updateCounts = append(updateCounts, delta)
		}
	}

	if len(updateIDs) > 0 {
		updateQuery := `
			UPDATE videos AS v
			SET like_count = like_count + unnested.delta
			FROM UNNEST($1::uuid[], $2::bigint[]) AS unnested(id, delta)
			WHERE v.id = unnested.id;
		`
		_, err := tx.Exec(ctx, updateQuery, updateIDs, updateCounts) // Note: using tx.Exec
		if err != nil {
			log.Printf("Worker %d: Bulk counter update failed: %v", workerID, err)
			return
		}
	}

	// COMMIT TRANSACTION
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Worker %d: Transaction commit failed: %v", workerID, err)
		return // Do not update Redis Cache or ACK if commit fails!
	}

	// 5. CACHE UPDATES & STREAM ACKNOWLEDGMENT (Only executed if DB succeeded)

	// Update the Redis Cache
	if len(updateIDs) > 0 {
		pipe := w.redis.Client.Pipeline()
		for i, vid := range updateIDs {
			counterKey := fmt.Sprintf("vid:%s:stats", vid)
			pipe.HIncrBy(ctx, counterKey, "likes", updateCounts[i])
		}
		_, _ = pipe.Exec(ctx)
	}

	// ACK the stream messages
	err = w.redis.Client.XAck(ctx, "stream:likes_ingest", "likes_worker_group", messageIDs...).Err()
	if err != nil {
		log.Printf("Worker %d: Failed to ACK stream messages (DB succeeded): %v", workerID, err)
	}
}
