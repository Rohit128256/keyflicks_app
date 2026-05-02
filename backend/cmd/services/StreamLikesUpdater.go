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
	State     string // "like" or "unlike"
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
func (w *StreamLikesWorker) Start(ctx context.Context, numWorkers int, numRouters int) {
	log.Printf("Starting StreamLikesWorker with %d partitioned workers and %d routers...", numWorkers, numRouters)

	// 1. Create the consumer group ONCE before starting routers
	streamKey := "stream:likes_ingest"
	groupName := "likes_worker_group"

	err := w.redis.Client.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	// 2. Create the slice of buffered channels
	workerChannels := make([]chan LikeEvent, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = make(chan LikeEvent, 1000)
		go w.workerLoop(ctx, i, workerChannels[i])
	}

	// 3. Start MULTIPLE stream consumers (routers)
	for i := 0; i < numRouters; i++ {
		consumerName := fmt.Sprintf("router_%d", i) // Unique name for each router
		go w.consumeStream(ctx, numWorkers, workerChannels, streamKey, groupName, consumerName)
	}

	<-ctx.Done()
	log.Println("StreamLikesWorker shutting down gracefully...")
}

func (w *StreamLikesWorker) consumeStream(ctx context.Context, numWorkers int, channels []chan LikeEvent, streamKey, groupName, consumerName string) {
	log.Printf("Router %s started...", consumerName)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Block for up to 2 seconds waiting for new messages
			streams, err := w.redis.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName, // Use the dynamically passed name
				Streams:  []string{streamKey, ">"},
				Count:    1000,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					continue // Stream empty, loop again
				}
				log.Printf("[%s] Error reading from stream: %v", consumerName, err)
				time.Sleep(1 * time.Second) // Backoff on error
				continue
			}

			// routing and hashing logic
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					vID, ok1 := msg.Values["video_id"].(string)
					uID, ok2 := msg.Values["user_id"].(string)
					state, ok3 := msg.Values["state"].(string)

					if !ok1 || !ok2 || !ok3 {
						log.Printf("Malformed message in stream %s, skipping...", msg.ID)
						w.redis.Client.XAck(ctx, streamKey, groupName, msg.ID)
						continue
					}

					event := LikeEvent{
						MessageID: msg.ID,
						VideoID:   vID,
						UserID:    uID,
						State:     state,
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

	var checkVids, checkUids []string

	for _, event := range batch {
		key := fmt.Sprintf("%s:%s", event.VideoID, event.UserID)
		if _, exists := collapsedState[key]; !exists {
			checkVids = append(checkVids, event.VideoID)
			checkUids = append(checkUids, event.UserID)
		}
		collapsedState[key] = event.State
		messageIDs = append(messageIDs, event.MessageID)
	}

	// START TRANSACTION
	tx, err := w.db.Begin(ctx)
	if err != nil {
		log.Printf("Worker %d: Failed to begin transaction: %v", workerID, err)
		return
	}
	defer tx.Rollback(ctx)

	// 2. THE FAST PRE-READ: Fetch current DB state for these specific users (Extremely fast via Index)
	dbStates := make(map[string]string)
	preReadQuery := `
		SELECT vl.video_id, vl.user_id, vl.type
		FROM video_likes vl
		INNER JOIN UNNEST($1::uuid[], $2::uuid[]) AS req(v_id, u_id)
		ON vl.user_id = req.u_id AND vl.video_id = req.v_id;
	`
	rows, err := tx.Query(ctx, preReadQuery, checkVids, checkUids)
	if err == nil {
		for rows.Next() {
			var v, u, t string
			if err := rows.Scan(&v, &u, &t); err == nil {
				dbStates[v+":"+u] = t
			}
		}
		rows.Close()
	}

	// 3. TRI-STATE DELTA MATH IN GO (Zero DB overhead)
	var upsertVids, upsertUids, upsertTypes []string
	var deleteVids, deleteUids []string
	likeDeltas := make(map[string]int64)
	dislikeDeltas := make(map[string]int64)

	for key, targetState := range collapsedState {
		parts := strings.Split(key, ":")
		vid, uid := parts[0], parts[1]

		currState := dbStates[key] // cuurent database state
		if currState == "" {
			currState = "none"
		}

		if currState == targetState {
			continue // Perfect sync, do nothing!
		}

		// Remove old state from counters
		if currState == "like" {
			likeDeltas[vid]--
		}
		if currState == "dislike" {
			dislikeDeltas[vid]--
		}

		// Add new state to counters
		if targetState == "like" {
			likeDeltas[vid]++
		}
		if targetState == "dislike" {
			dislikeDeltas[vid]++
		}

		// Route to Upsert or Delete arrays
		if targetState == "none" {
			deleteVids = append(deleteVids, vid)
			deleteUids = append(deleteUids, uid)
		} else {
			upsertVids = append(upsertVids, vid)
			upsertUids = append(upsertUids, uid)
			upsertTypes = append(upsertTypes, targetState)
		}
	}

	// 4. BULK UPSERT (Handles brand new likes/dislikes AND swapping types)
	if len(upsertVids) > 0 {
		upsertQuery := `
			INSERT INTO video_likes (user_id, video_id, type)
			SELECT unnested.user_id, unnested.video_id, unnested.type
			FROM UNNEST($1::uuid[], $2::uuid[], $3::varchar[]) AS unnested(user_id, video_id, type)
			ON CONFLICT (user_id, video_id) DO UPDATE SET type = EXCLUDED.type;
		`
		if _, err = tx.Exec(ctx, upsertQuery, upsertUids, upsertVids, upsertTypes); err != nil {
			log.Printf("Worker %d: Bulk upsert failed: %v", workerID, err)
			return
		}
	}

	// 5. BULK DELETE (Handles unliking/undisliking down to "none")
	if len(deleteVids) > 0 {
		deleteQuery := `
			DELETE FROM video_likes vl
			USING UNNEST($1::uuid[], $2::uuid[]) AS unnested(user_id, video_id)
			WHERE vl.user_id = unnested.user_id AND vl.video_id = unnested.video_id;
		`
		if _, err = tx.Exec(ctx, deleteQuery, deleteUids, deleteVids); err != nil {
			log.Printf("Worker %d: Bulk delete failed: %v", workerID, err)
			return
		}
	}

	// 6. BULK UPDATE BOTH COUNTERS
	var updateVids []string
	var updateLikes, updateDislikes []int64

	// Create a unique set of video IDs that need updates
	vidsToUpdate := make(map[string]bool)
	for v := range likeDeltas {
		vidsToUpdate[v] = true
	}
	for v := range dislikeDeltas {
		vidsToUpdate[v] = true
	}

	for vid := range vidsToUpdate {
		ld, dd := likeDeltas[vid], dislikeDeltas[vid]
		if ld != 0 || dd != 0 {
			updateVids = append(updateVids, vid)
			updateLikes = append(updateLikes, ld)
			updateDislikes = append(updateDislikes, dd)
		}
	}

	if len(updateVids) > 0 {
		// GREATEST(0) ensures counters mathematically cannot dip below 0 if DB gets out of sync
		updateQuery := `
			UPDATE videos AS v
			SET like_count = GREATEST(0, v.like_count + unnested.l_delta),
			    dislike_count = GREATEST(0, v.dislike_count + unnested.d_delta)
			FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[]) AS unnested(id, l_delta, d_delta)
			WHERE v.id = unnested.id;
		`
		if _, err := tx.Exec(ctx, updateQuery, updateVids, updateLikes, updateDislikes); err != nil {
			log.Printf("Worker %d: Bulk counter update failed: %v", workerID, err)
			return
		}
	}

	// 7. COMMIT TRANSACTION
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Worker %d: Transaction commit failed: %v", workerID, err)
		return
	}

	// 8. CACHE UPDATES & STREAM ACKNOWLEDGMENT
	if len(updateVids) > 0 {
		pipe := w.redis.Client.Pipeline()
		// LUA SCRIPT: Update BOTH cache fields ONLY if the hash is already warm.
		luaScript := `
			if redis.call("HEXISTS", KEYS[1], "likes") == 1 then
				redis.call("HINCRBY", KEYS[1], "likes", ARGV[1])
				redis.call("HINCRBY", KEYS[1], "dislikes", ARGV[2])
				return 1
			end
			return 0
		`
		for i, vid := range updateVids {
			counterKey := fmt.Sprintf("vid:%s:stats", vid)
			pipe.Eval(ctx, luaScript, []string{counterKey}, updateLikes[i], updateDislikes[i])
		}
		_, _ = pipe.Exec(ctx)
	}

	w.redis.Client.XAck(ctx, "stream:likes_ingest", "likes_worker_group", messageIDs...)
}
