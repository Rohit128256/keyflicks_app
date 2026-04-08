package services

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"keyflicks_app/internals/cache"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type CommentsDeleter struct {
	db         *pgxpool.Pool
	redis      *cache.RedisDB
	numWorkers int
}

func NewCommentsDeleter(db *pgxpool.Pool, redisCli *cache.RedisDB, numWorkers int) *CommentsDeleter {
	return &CommentsDeleter{
		db:         db,
		redis:      redisCli,
		numWorkers: numWorkers,
	}
}

func (s *CommentsDeleter) Start(ctx context.Context) {
	streamName := "stream:comments_delete"
	groupName := "comments_deleter_group"

	// 1. Create the Redis Consumer Group
	err := s.redis.Client.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Printf("CommentsDeleter: Could not create consumer group: %v", err)
	}

	log.Printf("Starting Comments Deleter with %d workers...", s.numWorkers)

	for i := 0; i < s.numWorkers; i++ {
		go s.worker(ctx, streamName, groupName, fmt.Sprintf("deleter-worker-%d", i))
	}

	<-ctx.Done()
}

func (s *CommentsDeleter) worker(ctx context.Context, stream, group, workerName string) {
	log.Printf("Comment deleter - %s has started", workerName)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Shutting down...", workerName)
			return
		default:
			// Block for up to 2 seconds waiting for delete events
			streams, err := s.redis.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: workerName,
				Streams:  []string{stream, ">"},
				Count:    100,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					log.Printf("[%s] Error reading stream: %v", workerName, err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			for _, streamMsg := range streams {
				s.processBatch(ctx, stream, group, workerName, streamMsg.Messages)
			}
		}
	}
}

func (s *CommentsDeleter) processBatch(ctx context.Context, stream, group, workerName string, messages []redis.XMessage) {
	if len(messages) == 0 {
		return
	}

	var msgIDs []string
	var commentIDs []string
	var userIDs []string

	// 1. Parse the payloads from Redis
	for _, msg := range messages {
		msgIDs = append(msgIDs, msg.ID)
		commentIDs = append(commentIDs, msg.Values["comment_id"].(string))
		userIDs = append(userIDs, msg.Values["user_id"].(string))
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		log.Printf("[%s] Failed to start transaction: %v", workerName, err)
		return
	}
	defer tx.Rollback(ctx)

	// 2. The Master Query: Recursive CTE to handle Cascading Deletes safely
	// It filters by user_id for security, finds all children, deletes them, and returns the stats.
	deleteQuery := `
		WITH RECURSIVE targets AS (
			-- Base: Find the explicit comments the user requested to delete (verifying ownership)
			SELECT c.id, c.video_id, c.parent_id
			FROM comments c
			INNER JOIN UNNEST($1::uuid[], $2::uuid[]) AS req(c_id, u_id)
			  ON c.id = req.c_id AND c.user_id = req.u_id
			
			UNION
			
			-- Recursive: Find all cascading replies belonging to those comments
			SELECT c.id, c.video_id, c.parent_id
			FROM comments c
			INNER JOIN targets t ON c.parent_id = t.id
		),
		deleted_rows AS (
			DELETE FROM comments
			WHERE id IN (SELECT id FROM targets)
			RETURNING video_id, parent_id
		)
		SELECT video_id, parent_id FROM deleted_rows;
	`

	rows, err := tx.Query(ctx, deleteQuery, commentIDs, userIDs)
	if err != nil {
		log.Printf("[%s] Bulk delete query failed: %v", workerName, err)
		return
	}

	videoCountsMap := make(map[string]int)
	replyCountsMap := make(map[string]int)

	// 3. Aggregate the accurate counts from the deleted rows
	for rows.Next() {
		var vID string
		var pID *string
		if err := rows.Scan(&vID, &pID); err != nil {
			log.Printf("[%s] Error scanning deleted row: %v", workerName, err)
			continue
		}

		videoCountsMap[vID]++
		if pID != nil {
			replyCountsMap[*pID]++
		}
	}
	rows.Close()

	// 4. Batch Decrement reply_counts (for surviving parent comments)
	if len(replyCountsMap) > 0 {
		var updateParentIDs []string
		var minusReplies []int

		for pID := range replyCountsMap {
			updateParentIDs = append(updateParentIDs, pID)
		}
		sort.Strings(updateParentIDs) // Sort to prevent deadlocks

		for _, pID := range updateParentIDs {
			minusReplies = append(minusReplies, replyCountsMap[pID])
		}

		// If a parent was deleted in the cascade, this safely does nothing for that specific ID
		updateReplyQuery := `
			UPDATE comments AS c
			SET reply_counts = GREATEST(0, c.reply_counts - unnested.minus_count)
			FROM UNNEST($1::uuid[], $2::int[]) AS unnested(parent_id, minus_count)
			WHERE c.id = unnested.parent_id;
		`
		_, err = tx.Exec(ctx, updateReplyQuery, updateParentIDs, minusReplies)
		if err != nil {
			log.Printf("[%s] Bulk reply decrement failed: %v", workerName, err)
			return
		}
	}

	// 5. Batch Decrement comment_count in videos table
	if len(videoCountsMap) > 0 {
		var updateVideoIDs []string
		var minusComments []int

		for vID := range videoCountsMap {
			updateVideoIDs = append(updateVideoIDs, vID)
		}
		sort.Strings(updateVideoIDs) // Sort to prevent deadlocks

		for _, vID := range updateVideoIDs {
			minusComments = append(minusComments, videoCountsMap[vID])
		}

		updateVideoQuery := `
			UPDATE videos AS v
			SET comment_count = GREATEST(0, v.comment_count - unnested.minus_count)
			FROM UNNEST($1::uuid[], $2::int[]) AS unnested(video_id, minus_count)
			WHERE v.id = unnested.video_id;
		`
		_, err = tx.Exec(ctx, updateVideoQuery, updateVideoIDs, minusComments)
		if err != nil {
			log.Printf("[%s] Bulk video comment decrement failed: %v", workerName, err)
			return
		}
	}

	// 6. Commit the transaction
	if err = tx.Commit(ctx); err != nil {
		log.Printf("[%s] Failed to commit transaction: %v", workerName, err)
		return
	}

	// 7. Acknowledge messages
	err = s.redis.Client.XAck(ctx, stream, group, msgIDs...).Err()
	if err != nil {
		log.Printf("[%s] Failed to XAck messages: %v", workerName, err)
	}
}
