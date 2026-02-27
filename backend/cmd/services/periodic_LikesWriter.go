package services

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"keyflicks_app/internals/cache"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type LikeSyncer struct {
	db    *pgxpool.Pool
	redis *cache.RedisDB // Using your custom Redis wrapper
}

func NewLikeSyncer(db *pgxpool.Pool, redisCli *cache.RedisDB) *LikeSyncer {
	return &LikeSyncer{
		db:    db,
		redis: redisCli,
	}
}

// Start runs continuously until the context is cancelled
func (s *LikeSyncer) Start(ctx context.Context) {
	log.Println("Starting Like Syncer service...")

	// Wake up every 5 seconds to process a batch
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Like Syncer: Shutdown signal received...")
			return
		case <-ticker.C:
			s.processBatch(ctx)
		}
	}
}

func (s *LikeSyncer) processBatch(ctx context.Context) {
	dirtySetKey := "sync:dirty_interactions"

	items, err := s.redis.PopFromSet(ctx, dirtySetKey, 100)
	if err != nil || len(items) == 0 {
		return
	}

	// 1. Prepare Go slices to hold the separated data
	var insertUsers, insertVideos []string
	var deleteUsers, deleteVideos []string

	for _, item := range items {
		parts := strings.Split(item, ":")
		if len(parts) != 2 {
			continue
		}
		vID, uID := parts[0], parts[1]

		stateKey := fmt.Sprintf("vid:%s:user:%s", vID, uID)
		val, err := s.redis.Get(ctx, stateKey)

		// 2. Sort the items into their respective slices
		if err == redis.Nil || val == "" {
			deleteUsers = append(deleteUsers, uID)
			deleteVideos = append(deleteVideos, vID)
		} else if val == "like" {
			insertUsers = append(insertUsers, uID)
			insertVideos = append(insertVideos, vID)
		}
	}

	dbFailed := false

	// 3. SINGLE QUERY INSERT
	// Unnest converts the arrays into a temporary table in memory,
	// joins it with your users table, and bulk inserts it.
	if len(insertUsers) > 0 {
		insertQuery := `
			INSERT INTO video_likes (user_id, video_id, type)
			SELECT unnested.user_id, unnested.video_id, 'like'
			FROM UNNEST($1::uuid[], $2::uuid[]) AS unnested(user_id, video_id)
			ON CONFLICT (user_id, video_id) DO NOTHING;
		`
		// Note: If your video_id in DB is UUID, change $2::text[] to $2::uuid[]
		_, err := s.db.Exec(ctx, insertQuery, insertUsers, insertVideos)
		if err != nil {
			log.Printf("LikeSyncer: Bulk insert failed: %v", err)
			dbFailed = true
		}
	}

	// 4. SINGLE QUERY DELETE
	if len(deleteUsers) > 0 {
		deleteQuery := `
			DELETE FROM video_likes vl
			USING UNNEST($1::uuid[], $2::uuid[]) AS unnested(user_id, video_id)
			WHERE vl.user_id = unnested.user_id AND vl.video_id = unnested.video_id;
		`
		_, err := s.db.Exec(ctx, deleteQuery, deleteUsers, deleteVideos)
		if err != nil {
			log.Printf("LikeSyncer: Bulk delete failed: %v", err)
			dbFailed = true
		}
	}

	// 5. Retry Logic (All or Nothing)
	if dbFailed {
		anyItems := make([]any, len(items))

		for i, v := range items {
			anyItems[i] = v
		}
		err := s.redis.AddToSet(ctx, dirtySetKey, anyItems...) // Push the whole batch back
		if err != nil {
			log.Printf("CRITICAL: Failed to push items back to dirty set: %v", err)
		}
	}
}
