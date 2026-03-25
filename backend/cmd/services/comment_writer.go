package services

import (
	"context"
	"fmt"
	"log"
	"time"

	"keyflicks_app/internals/cache"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type CommentsWriter struct {
	db         *pgxpool.Pool
	redis      *cache.RedisDB
	numWorkers int
}

func NewCommentsWriter(db *pgxpool.Pool, redisCli *cache.RedisDB, numWorkers int) *CommentsWriter {
	return &CommentsWriter{
		db:         db,
		redis:      redisCli,
		numWorkers: numWorkers,
	}
}

// Start launches multiple independent workers listening to the stream
func (s *CommentsWriter) Start(ctx context.Context) {
	streamName := "stream:comments_ingest"
	groupName := "comments_workers_group"

	// 1. Create the Redis Consumer Group (Ignore error if it already exists)
	err := s.redis.Client.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Printf("CommentsWriter: Could not create consumer group: %v", err)
	}

	log.Printf("Starting Comments Writer with %d workers...", s.numWorkers)

	for i := 0; i < s.numWorkers; i++ {
		go s.worker(ctx, streamName, groupName, fmt.Sprintf("comment-worker-%d", i))
	}

	<-ctx.Done()
}

func (s *CommentsWriter) worker(ctx context.Context, stream, group, workerName string) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Shutting down...", workerName)
			return
		default:
			// Block for up to 2 seconds waiting for new comments
			streams, err := s.redis.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: workerName,
				Streams:  []string{stream, ">"}, // ">" means give me messages never delivered to other consumers
				Count:    100,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil { // redis.Nil just means timeout/no new messages, which is normal
					log.Printf("[%s] Error reading stream: %v", workerName, err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			// Process the batches
			for _, streamMsg := range streams {
				s.processBatch(ctx, stream, group, workerName, streamMsg.Messages)
			}
		}
	}
}

func (s *CommentsWriter) processBatch(ctx context.Context, stream, group, workerName string, messages []redis.XMessage) {
	if len(messages) == 0 {
		return
	}

	var userIDs []string
	var videoIDs []string
	var texts []string
	var parentIDs []*string // Pointers are used so we can pass 'nil' to Postgres for NULL
	var createdAts []time.Time

	replyCountsMap := make(map[string]int)
	var msgIDs []string

	// 1. Parse the payloads from Redis
	for _, msg := range messages {
		msgIDs = append(msgIDs, msg.ID)

		userIDs = append(userIDs, msg.Values["user_id"].(string))
		videoIDs = append(videoIDs, msg.Values["video_id"].(string))
		texts = append(texts, msg.Values["text"].(string))

		// Parse timestamp back to time.Time
		tStr := msg.Values["created_at"].(string)
		t, _ := time.Parse(time.RFC3339, tStr)
		createdAts = append(createdAts, t)

		// Safely handle parent_id if it's a reply
		if pID, ok := msg.Values["parent_id"].(string); ok && pID != "" {
			parentIDs = append(parentIDs, &pID)
			replyCountsMap[pID]++ // Tally the replies for the batch update
		} else {
			parentIDs = append(parentIDs, nil)
		}
	}

	// 2. Start a Database Transaction
	tx, err := s.db.Begin(ctx)
	if err != nil {
		log.Printf("[%s] Failed to start transaction: %v", workerName, err)
		return
	}
	defer tx.Rollback(ctx)

	// 3. Batch Insert all 100 comments instantly
	insertQuery := `
		INSERT INTO comments (user_id, video_id, text, parent_id, created_at)
		SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::uuid[], $5::timestamptz[])
	`
	_, err = tx.Exec(ctx, insertQuery, userIDs, videoIDs, texts, parentIDs, createdAts)
	if err != nil {
		log.Printf("[%s] Bulk insert failed: %v", workerName, err)
		return // Function exits, defer rolls back, messages stay in queue
	}

	// 4. Batch Increment reply_counts (if there are replies in this batch)
	if len(replyCountsMap) > 0 {
		var updateParentIDs []string
		var newReplies []int

		for pID, count := range replyCountsMap {
			updateParentIDs = append(updateParentIDs, pID)
			newReplies = append(newReplies, count)
		}

		updateQuery := `
			UPDATE comments AS c
			SET reply_counts = c.reply_counts + unnested.new_replies
			FROM UNNEST($1::uuid[], $2::int[]) AS unnested(parent_id, new_replies)
			WHERE c.id = unnested.parent_id;
		`
		_, err = tx.Exec(ctx, updateQuery, updateParentIDs, newReplies)
		if err != nil {
			log.Printf("[%s] Bulk reply increment failed: %v", workerName, err)
			return // Function exits, defer rolls back, messages stay in queue
		}
	}

	// 5. Commit the transaction
	if err = tx.Commit(ctx); err != nil {
		log.Printf("[%s] Failed to commit transaction: %v", workerName, err)
		return
	}

	// 6. Acknowledge the messages to remove them from the Redis pending queue
	err = s.redis.Client.XAck(ctx, stream, group, msgIDs...).Err()
	if err != nil {
		log.Printf("[%s] Failed to XAck messages: %v", workerName, err)
	}
}
