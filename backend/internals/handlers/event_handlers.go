package handlers

import (
	"context"
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

	if !exists {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}

	currUser := user.(*schemas.UserInDB)
	curruserId := currUser.ID.String()

	// defining the keys for counter and sets
	stateKey := fmt.Sprintf("vid:%s:user:%s", video_id, curruserId)
	dirtyStateKey := "sync:dirty_interactions"
	dirtyLikeKey := "sync:dirty_video_counts"
	counterKey := fmt.Sprintf("vid:%s:stats", video_id)

	// warm the cache counter
	if h.redis.Client.Exists(c, counterKey).Val() == 0 {
		// Cache is cold. Fetch the true base value from the database.
		videoInfo, err := h.db.GetVideoDetails(c, video_id)

		if err != nil {
			log.Printf("Error fetching video details for caching: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "This service is not allowed currently!"})
			return
		}

		h.redis.Client.HSetNX(c, counterKey, "likes", videoInfo.Likes)

	}

	currentState := h.redis.Client.Get(c, stateKey).Val()

	if actionType == "like" && currentState == "like" {
		// user already liked it !
		c.Status(http.StatusNoContent)
		return
	}

	incrCount := 0
	if actionType == "like" {
		incrCount = 1
	} else {
		incrCount = -1
	}

	pipe := h.redis.Pipeline()

	if actionType == "like" {
		pipe.Set(c, stateKey, "like", 5*time.Hour)
		pipe.SAdd(c, dirtyStateKey, fmt.Sprintf("%s:%s", video_id, curruserId))
	} else {
		pipe.Del(c, stateKey)
		pipe.SAdd(c, dirtyStateKey, fmt.Sprintf("%s:%s", video_id, curruserId))
	}

	pipe.HIncrBy(c, counterKey, "likes", int64(incrCount))
	pipe.Expire(c, counterKey, 30*time.Minute)

	// We can safely add to the dirty set now because the base count is guaranteed to be correct
	pipe.SAdd(c, dirtyLikeKey, video_id)

	_, err := pipe.Exec(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Redis pipeline failed"})
		return
	}

	c.Status(http.StatusNoContent)

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

	// 1. Fetch both states from Redis in ONE network trip
	pipe := h.redis.Pipeline()
	stateCmd := pipe.Get(c, stateKey)
	countCmd := pipe.HGet(c, counterKey, "likes")
	_, _ = pipe.Exec(c)

	// 2. Parse User Like State from Cache
	currUserLiked := false
	if stateCmd.Err() == nil && stateCmd.Val() == "like" {
		currUserLiked = true
	}

	// 3. Parse Total Likes from Cache
	var videoLikes int64
	countErr := countCmd.Err()

	if countErr == nil {
		videoLikes, _ = countCmd.Int64()

		// THE PARANOIA CHECK: Guard against the blind-increment race condition
		if videoLikes == 1 || videoLikes == -1 {
			countErr = redis.Nil // Forcing a database read
		}
	}

	// 4. Database Fallback (Cache Miss or Paranoia Trigger)
	if countErr == redis.Nil || countErr != nil {

		// Use your new clean DbStore method
		dbState, err := h.db.GetLikeState(c, video_id, curruserId)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Video not found"})
			return
		}

		// Map the DB results to our local variables
		videoLikes = dbState.VideoLikes
		currUserLiked = dbState.CurrUserLiked

		// 5. Asynchronously heal the cache so the next user gets it instantly
		go func(vID string, likes int64) {
			bgCtx := context.Background()
			cKey := fmt.Sprintf("vid:%s:stats", vID)

			healPipe := h.redis.Pipeline()
			healPipe.HSetNX(bgCtx, cKey, "likes", likes)
			healPipe.Expire(bgCtx, cKey, 30*time.Minute)
			_, _ = healPipe.Exec(bgCtx)
		}(video_id, videoLikes)
	}

	// Returning the perfectly accurate payload
	c.JSON(http.StatusOK, gin.H{
		"videoLikes":    videoLikes,
		"currUserLiked": currUserLiked,
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
	cursorStr := c.Query("cursor")

	user, exists := c.Get("currentUser")
	if !exists || videoID == "" {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}

	currUser := user.(*schemas.UserInDB)
	currUserID := currUser.ID.String() // We need this now!

	// 1. Parse Inputs safely
	var parentID *string
	if parentIDStr != "" {
		parentID = &parentIDStr
	}

	var cursor *time.Time
	if cursorStr != "" {
		parsedTime, err := time.Parse(time.RFC3339, cursorStr)
		if err == nil {
			cursor = &parsedTime
		}
	}

	limit := 20

	// 2. Cache Logic
	var comments []schemas.CommentResponse
	var err error

	isFirstPageTopLevel := parentID == nil && cursor == nil

	if isFirstPageTopLevel {
		comments, err = h.redis.GetFirstPageComments(c.Request.Context(), videoID)
	}

	// 3. Database Fallback
	if len(comments) == 0 {
		comments, err = h.db.GetComments(c.Request.Context(), videoID, parentID, cursor, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch comments"})
			return
		}

		if isFirstPageTopLevel && len(comments) > 0 {
			go h.redis.SetFirstPageComments(videoID, comments)
		}
	}

	// ---------------------------------------------------------
	// NEW: 3.5 PULL CURRENT USER'S COMMENT TO TOP (First Page Only)
	// ---------------------------------------------------------
	if isFirstPageTopLevel {
		userComments, err := h.db.GetUserTopLevelComments(c.Request.Context(), videoID, currUserID)

		if err == nil && len(userComments) > 0 {
			// 1. Create a fast-lookup map of the user's comment IDs
			userCommentIDs := make(map[string]bool, len(userComments))
			for _, uc := range userComments {
				userCommentIDs[uc.ID] = true
			}

			// 2. Filter out those exact comments from the main cached/DB list
			filteredComments := make([]schemas.CommentResponse, 0, len(comments))
			for _, c := range comments {
				if !userCommentIDs[c.ID] {
					filteredComments = append(filteredComments, c)
				}
			}

			// 3. Prepend ALL user comments to the top of the final list
			comments = append(userComments, filteredComments...)
		}
	}

	// 4. Calculate Next Cursor
	var nextCursor *time.Time
	if len(comments) >= limit { // Changed to >= because we might have prepended one, making len 21
		lastCommentTime := comments[len(comments)-1].CreatedAt
		nextCursor = &lastCommentTime
	}

	// 5. Send Response
	c.JSON(http.StatusOK, schemas.PaginatedComments{
		Comments:   comments,
		NextCursor: nextCursor,
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
		Values: map[string]interface{}{
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
	// (Assuming you have a method like DeleteFirstPageComments or you can use the raw client)
	// This ensures the deleted comment vanishes immediately if the user reloads the page.
	cacheKey := fmt.Sprintf("comments_first_page:%s", req.VideoID) // Adjust this key to match what SetFirstPageComments uses
	h.redis.Client.Del(c.Request.Context(), cacheKey)

	// 4. Return instant success
	c.JSON(http.StatusAccepted, gin.H{
		"message": "Comment queued for deletion",
	})
}
