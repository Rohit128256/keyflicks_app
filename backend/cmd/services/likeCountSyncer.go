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

type CounterSyncer struct {
	db    *pgxpool.Pool
	redis *cache.RedisDB
}

func NewCounterSyncer(db *pgxpool.Pool, redisCli *cache.RedisDB) *CounterSyncer {
	return &CounterSyncer{
		db:    db,
		redis: redisCli,
	}
}

// Start launches multiple workers to update the database periodically
func (s *CounterSyncer) Start(ctx context.Context, numWorkers int) {
	log.Printf("Starting Counter Syncer with %d workers...", numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			// A 10-second ticker is perfect for counters.
			//We don't need real-time DB writes because Redis handles the real-time reads!
			ticker := time.NewTicker(20 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					log.Printf("Counter Syncer Worker %d shutting down...", workerID)
					return
				case <-ticker.C:
					s.processBatch(ctx)
				}
			}
		}(i)
	}

	<-ctx.Done()
}

func (s *CounterSyncer) processBatch(ctx context.Context) {
	dirtySetKey := "sync:dirty_video_counts"

	// 1. Pop up to 500 unique videos that need updating
	videoIDs, err := s.redis.PopFromSet(ctx, dirtySetKey, 500)
	if err != nil || len(videoIDs) == 0 {
		return
	}

	// 2. Fetch all their exact counts from Redis in ONE pipeline trip
	pipe := s.redis.Pipeline()
	cmds := make(map[string]*redis.StringCmd)

	for _, vID := range videoIDs {
		counterKey := fmt.Sprintf("vid:%s:stats", vID)
		cmds[vID] = pipe.HGet(ctx, counterKey, "likes")
	}

	// Execute the pipeline (ignore global error, we will check individual commands)
	_, _ = pipe.Exec(ctx)

	// 3. Prepare slices for the Postgres bulk update
	var updateIDs []string
	var updateCounts []int64

	for vID, cmd := range cmds {
		count, err := cmd.Int64()
		// If err == redis.Nil, the key expired before we read it (very rare with a 30m window).
		// We just safely skip it. The DB already has the last known good state.
		if err == nil {
			updateIDs = append(updateIDs, vID)
			updateCounts = append(updateCounts, count)
		}
	}

	if len(updateIDs) == 0 {
		return
	}

	// 4. BULK UPDATE QUERY - Blazing fast, zero locks between workers
	updateQuery := `
		UPDATE videos AS v
		SET like_count = unnested.count
		FROM UNNEST($1::uuid[], $2::bigint[]) AS unnested(id, count)
		WHERE v.id = unnested.id;
	`

	_, err = s.db.Exec(ctx, updateQuery, updateIDs, updateCounts)

	// 5. Retry logic if the DB is down
	if err != nil {
		log.Printf("CounterSyncer: Bulk update failed: %v", err)

		// Push the original videoIDs back into the set to try again later
		anyItems := make([]any, len(videoIDs))
		for i, v := range videoIDs {
			anyItems[i] = v
		}
		_ = s.redis.AddToSet(ctx, dirtySetKey, anyItems...)
	}
}
