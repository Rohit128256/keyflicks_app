package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"keyflicks_app/internals/cache"
	database "keyflicks_app/internals/db"
	"keyflicks_app/internals/schemas"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gin-gonic/gin"
)

type EventHandler struct {
	db    *database.DbStore
	redis *cache.RedisDB
}

func NewEventHandler(db *database.DbStore, redis *cache.RedisDB) *EventHandler {
	return &EventHandler{db: db, redis: redis}
}

// Like counter handler
func (h *EventHandler) ToggleLike(c *gin.Context) {
	video_id := c.Query("video_id")
	actionType := c.Query("action")
	user, exists := c.Get("currentUser")

	// 1. Validate the 4 exact actions
	if actionType != "like" && actionType != "unlike" && actionType != "dislike" && actionType != "undislike" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action type"})
		return
	}

	if !exists {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}

	currUser := user.(*schemas.UserInDB)
	curruserId := currUser.ID.String()

	stateKey := fmt.Sprintf("vid:%s:user:%s", video_id, curruserId)
	streamKey := "stream:likes_ingest"

	// 2. Map Action to Absolute Tri-State
	targetState := "none" // Default for "unlike" and "undislike"
	if actionType == "like" {
		targetState = "like"
	} else if actionType == "dislike" {
		targetState = "dislike"
	}

	// 3. Read Current Optimistic State (Fast Cache Read)
	currentState, err := h.redis.Client.Get(c.Request.Context(), stateKey).Result()
	if err == redis.Nil || currentState == "" {
		currentState = "none"
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Cache error"})
		return
	}

	// 4. Debounce Spammers Instantly
	if currentState == targetState {
		c.Status(http.StatusNoContent)
		return
	}

	// 5. Update personal state & push to stream in ONE network trip
	pipe := h.redis.Client.Pipeline()

	// Set the state so subsequent clicks are instantly debounced above
	pipe.Set(c.Request.Context(), stateKey, targetState, 5*time.Hour)

	// Push the final state to the background worker
	pipe.XAdd(c.Request.Context(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"video_id": video_id,
			"user_id":  curruserId,
			"state":    targetState,
		},
	})

	_, pipeErr := pipe.Exec(c.Request.Context())
	if pipeErr != nil {
		log.Printf("Failed to process reaction pipeline for video %s: %v", video_id, pipeErr)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue action"})
		return
	}

	c.Status(http.StatusAccepted)
}

// get Like details from this handler
func (h *EventHandler) Getlikes(c *gin.Context) {
	video_id := c.Query("video_id")
	user, exists := c.Get("currentUser")

	if !exists || video_id == "" {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}

	currUser := user.(*schemas.UserInDB)
	curruserId := currUser.ID.String()

	stateKey := fmt.Sprintf("vid:%s:user:%s", video_id, curruserId)
	counterKey := fmt.Sprintf("vid:%s:stats", video_id)

	// fetch both from Redis in single trip
	pipe := h.redis.Client.Pipeline()
	stateCmd := pipe.Get(c.Request.Context(), stateKey)
	countCmd := pipe.HMGet(c.Request.Context(), counterKey, "likes", "dislikes")
	_, _ = pipe.Exec(c.Request.Context())

	// evaluating cache misses Separately!
	counts, countErr := countCmd.Result()
	counterMiss := countErr == redis.Nil || len(counts) < 2 || counts[0] == nil || counts[1] == nil
	stateMiss := stateCmd.Err() == redis.Nil

	var videoLikes, videoDislikes int64
	currUserLiked, currUserDisliked := false, false

	// fallback only for missing Counters
	if counterMiss {
		// using existing DB method!
		videoInfo, err := h.db.GetVideoDetails(c.Request.Context(), video_id)
		if err == nil {
			videoLikes = videoInfo.Likes
			videoDislikes = videoInfo.Dislikes

			// Heal Counter Cache
			go func(vID string, l, d int64) {
				bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				cKey := fmt.Sprintf("vid:%s:stats", vID)
				healPipe := h.redis.Client.Pipeline()
				healPipe.HSetNX(bgCtx, cKey, "likes", l)
				healPipe.HSetNX(bgCtx, cKey, "dislikes", d)
				healPipe.Expire(bgCtx, cKey, 30*time.Minute)
				_, _ = healPipe.Exec(bgCtx)
			}(video_id, videoLikes, videoDislikes)
		}
	} else {
		// Cache Hit
		if likesStr, ok := counts[0].(string); ok {
			fmt.Sscanf(likesStr, "%d", &videoLikes)
		}
		if dislikesStr, ok := counts[1].(string); ok {
			fmt.Sscanf(dislikesStr, "%d", &videoDislikes)
		}
	}

	// fallback only for missing User State
	if stateMiss {
		// Microscopic point-read just for this user
		dbState, _ := h.db.GetUserReaction(c.Request.Context(), video_id, curruserId)
		currUserLiked = dbState == "like"
		currUserDisliked = dbState == "dislike"

		// Heal User State Cache (Explicitly cache "none" so they never hit the DB again)
		go func(vID, uID, state string) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			sKey := fmt.Sprintf("vid:%s:user:%s", vID, uID)
			h.redis.Client.Set(bgCtx, sKey, state, 5*time.Hour)
		}(video_id, curruserId, dbState)

	} else {
		// Cache Hit
		cachedState := stateCmd.Val()
		currUserLiked = cachedState == "like"
		currUserDisliked = cachedState == "dislike"
	}

	c.JSON(http.StatusOK, gin.H{
		"videoLikes":       videoLikes,
		"currUserLiked":    currUserLiked,
		"VideoDislikes":    videoDislikes,
		"currUserDisliked": currUserDisliked,
	})
}

func (h *EventHandler) PostComment(c *gin.Context) {
	user, exists := c.Get("currentUser")

	if !exists {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}

	currUser := user.(*schemas.UserInDB)
	curruserId := currUser.ID.String()

	// Bind the JSON payload
	var commentPayload schemas.VideoComment
	if err := c.ShouldBindJSON(&commentPayload); err != nil || commentPayload.CommentText == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid or empty comment text"})
		return
	}

	video_id := commentPayload.ID
	comment_text := commentPayload.CommentText

	if video_id == "" || comment_text == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid or empty required field"})
		return
	}

	// Checking if this is a reply to another comment
	parent_id := commentPayload.PID

	// Prepare the payload for the Redis Stream
	streamKey := "stream:comments_ingest"

	values := map[string]any{
		"video_id": video_id,
		"user_id":  curruserId,
		"text":     comment_text,
		// Storing the exact timestamp now so the DB reflects exactly when the user clicked "post"
		"created_at": time.Now().UTC().Format(time.RFC3339),
	}

	// Only add parent_id to the stream if it actually exists
	if parent_id != "" {
		values["parent_id"] = parent_id
	}

	// Push to the Redis Stream instantly using go-redis
	err := h.redis.Client.XAdd(c, &redis.XAddArgs{
		Stream: streamKey,
		Values: values,
	}).Err()

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process comment"})
		return
	}

	// Return instantly to the user (202 Accepted indicates it's queued for processing)
	c.JSON(http.StatusAccepted, gin.H{
		"message": "Comment posted successfully",
	})
}

func (h *EventHandler) GetComments(c *gin.Context) {
	videoID := c.Query("video_id")
	parentIDStr := c.Query("parent_id")
	cursorTimeStr := c.Query("cursor_time")
	cursorIDStr := c.Query("cursor_id")

	user, exists := c.Get("currentUser")
	if !exists || videoID == "" {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}

	currUser := user.(*schemas.UserInDB)
	currUserID := currUser.ID.String()

	var parentID *string
	if parentIDStr != "" {
		parentID = &parentIDStr
	}

	var cursorTime *time.Time
	var cursorID *string
	if cursorTimeStr != "" && cursorIDStr != "" {
		parsedTime, _ := time.Parse(time.RFC3339, cursorTimeStr)
		cursorTime, cursorID = &parsedTime, &cursorIDStr
	}

	limit := 20
	var comments []schemas.CommentResponse
	var err error

	isFirstPageTopLevel := parentID == nil && cursorTime == nil

	var liveReplyCounts map[string]string
	var newTopCommentsStr []string

	// 1. Fetch Snapshot + Buffers in ONE Network Trip!
	if isFirstPageTopLevel {
		pipe := h.redis.Client.Pipeline()
		snapCmd := pipe.Get(c.Request.Context(), fmt.Sprintf("video:%s:comments:first_page", videoID))
		newTopCmd := pipe.LRange(c.Request.Context(), fmt.Sprintf("video:%s:new_top_comments", videoID), 0, -1)
		liveRepCmd := pipe.HGetAll(c.Request.Context(), fmt.Sprintf("video:%s:live_reply_counts", videoID))

		_, _ = pipe.Exec(c.Request.Context())

		if snapCmd.Err() == nil {
			json.Unmarshal([]byte(snapCmd.Val()), &comments)
		}
		newTopCommentsStr = newTopCmd.Val()
		liveReplyCounts = liveRepCmd.Val()
	}

	// 2. Database Fallback (Self-Healing)
	if len(comments) == 0 {
		comments, err = h.db.GetComments(c.Request.Context(), videoID, parentID, cursorTime, cursorID, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch comments"})
			return
		}

		if isFirstPageTopLevel && len(comments) > 0 {
			go func(vID string, data []schemas.CommentResponse) {
				bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				b, _ := json.Marshal(data)
				// Create the Snapshot, and WIPE the buffers so the cycle restarts perfectly!
				pipe := h.redis.Client.Pipeline()
				pipe.Set(bgCtx, fmt.Sprintf("video:%s:comments:first_page", vID), string(b), 5*time.Minute)
				pipe.Del(bgCtx, fmt.Sprintf("video:%s:new_top_comments", vID))
				pipe.Del(bgCtx, fmt.Sprintf("video:%s:live_reply_counts", vID))
				pipe.Exec(bgCtx)
			}(videoID, comments)
		}
	} else if isFirstPageTopLevel {
		// 3. CACHE HIT: Stitch the Deltas!
		if len(newTopCommentsStr) > 0 {
			var merged []schemas.CommentResponse
			seen := make(map[string]bool)

			// Prepend brand new comments first
			for _, str := range newTopCommentsStr {
				var cm schemas.CommentResponse
				if err := json.Unmarshal([]byte(str), &cm); err == nil && !seen[cm.ID] {
					merged = append(merged, cm)
					seen[cm.ID] = true
				}
			}
			// Append the old snapshot comments
			for _, cm := range comments {
				if !seen[cm.ID] {
					merged = append(merged, cm)
					seen[cm.ID] = true
				}
			}
			comments = merged
			if len(comments) > limit {
				comments = comments[:limit]
			}
		}

		// Apply the Live Reply Counters seamlessly
		if len(liveReplyCounts) > 0 {
			for i := range comments {
				if deltaStr, exists := liveReplyCounts[comments[i].ID]; exists {
					var delta int64
					fmt.Sscanf(deltaStr, "%d", &delta)
					comments[i].ReplyCounts += delta
				}
			}
		}
	}

	// 4. Pull Current User's Comment to Top
	if isFirstPageTopLevel {
		userComments, err := h.db.GetUserTopLevelComments(c.Request.Context(), videoID, currUserID)
		if err == nil && len(userComments) > 0 {
			userCommentIDs := make(map[string]bool, len(userComments))
			for _, uc := range userComments {
				userCommentIDs[uc.ID] = true
			}

			filteredComments := make([]schemas.CommentResponse, 0, len(comments))
			for _, c := range comments {
				if !userCommentIDs[c.ID] {
					filteredComments = append(filteredComments, c)
				}
			}
			comments = append(userComments, filteredComments...)
		}
	}

	// 5. Setup Next Cursors
	var nextCursorTime *time.Time
	var nextCursorID *string
	if len(comments) >= limit {
		lastComment := comments[len(comments)-1]
		nextCursorTime = &lastComment.CreatedAt
		nextCursorID = &lastComment.ID
	}

	c.JSON(http.StatusOK, gin.H{
		"comments":         comments,
		"next_cursor_time": nextCursorTime,
		"next_cursor_id":   nextCursorID,
	})
}

func (h *EventHandler) DeleteComment(c *gin.Context) {
	user, exists := c.Get("currentUser")
	if !exists {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}

	currUser := user.(*schemas.UserInDB)
	currUserID := currUser.ID.String()

	// 1. Bind and Validate Payload
	var req schemas.DeleteCommentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload. comment_id and video_id are required."})
		return
	}

	// 2. Push to the Delete Stream
	streamKey := "stream:comments_delete"

	err := h.redis.Client.XAdd(c.Request.Context(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"comment_id": req.CommentID,
			"user_id":    currUserID,
		},
	}).Err()

	if err != nil {
		log.Printf("Failed to push delete event to stream for comment %s: %v", req.CommentID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue comment for deletion"})
		return
	}

	// 3. Invalidate the Cache Instantly
	pipe := h.redis.Client.Pipeline()
	pipe.Del(c.Request.Context(), fmt.Sprintf("video:%s:comments:first_page", req.VideoID))
	pipe.Del(c.Request.Context(), fmt.Sprintf("video:%s:new_top_comments", req.VideoID))
	pipe.Del(c.Request.Context(), fmt.Sprintf("video:%s:live_reply_counts", req.VideoID))
	_, _ = pipe.Exec(c.Request.Context())

	// 4. Return instant success
	c.JSON(http.StatusAccepted, gin.H{
		"message": "Comment queued for deletion",
	})
}

func (h *EventHandler) GetCommentsCount(c *gin.Context) {
	video_id := c.Query("video_id")

	if video_id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "video_id is required"})
		return
	}

	counterKey := fmt.Sprintf("vid:%s:stats", video_id)

	// 1. Fast Cache Read
	countCmd := h.redis.Client.HGet(c.Request.Context(), counterKey, "comments")

	var videoComments int64
	needsDBFallback := false

	if countCmd.Err() == nil {
		videoComments, _ = countCmd.Int64()
	} else if countCmd.Err() == redis.Nil {
		needsDBFallback = true
	} else {
		// Log actual connection/network errors but still fallback to DB to save the UX
		log.Printf("Redis error fetching comments count for %s: %v", video_id, countCmd.Err())
		needsDBFallback = true
	}

	// 2. Database Fallback (Source of Truth)
	if needsDBFallback {
		// Using your existing db method!
		videoInfo, err := h.db.GetVideoDetails(c.Request.Context(), video_id)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Video not found"})
			return
		}

		videoComments = videoInfo.Comments

		// 3. Asynchronously HEAL THE CACHE
		go func(vID string, comments int64) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			cKey := fmt.Sprintf("vid:%s:stats", vID)

			healPipe := h.redis.Client.Pipeline()

			// HSetNX is CRUCIAL here. It guarantees we only set the cache if it doesn't exist.
			// This prevents this DB fallback from accidentally overwriting a newer comment count
			// that the `comment_writer.go` worker might have *just* pushed 1 millisecond ago!
			healPipe.HSetNX(bgCtx, cKey, "comments", comments)
			healPipe.Expire(bgCtx, cKey, 30*time.Minute)

			_, err := healPipe.Exec(bgCtx)
			if err != nil {
				log.Printf("Failed to heal comments cache for video %s: %v", vID, err)
			}
		}(video_id, videoComments)
	}

	// 4. Return the perfectly accurate payload
	c.JSON(http.StatusOK, gin.H{
		"comment_counts": videoComments,
	})
}
