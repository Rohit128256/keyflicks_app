package handlers

import (
	"fmt"
	"keyflicks_app/internals/cache"
	database "keyflicks_app/internals/db"
	"keyflicks_app/internals/schemas"
	"net/http"
	"time"

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

	video_id := c.Param("video_id")
	actionType := c.Param("action")
	user, exists := c.Get("currentUser")

	if !exists {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}

	currUser := user.(*schemas.UserInDB)

	curruserName := currUser.Username

	// defining the keys for counter and sets
	stateKey := fmt.Sprintf("vid:%s:user:%s", video_id, curruserName)
	dirtyStateKey := "sync:dirty_interactions"
	counterKey := fmt.Sprintf("vid:%s:stats", video_id)

	incrCount := 0
	if actionType == "like" {
		incrCount = 1
	} else {
		incrCount = -1
	}

	pipe := h.redis.Pipeline()

	if actionType == "like" {
		pipe.Set(c, stateKey, "like", 5*time.Hour)
		pipe.SAdd(c, dirtyStateKey, fmt.Sprintf("%s:%s", video_id, curruserName))
	} else {
		pipe.Del(c, stateKey)
		pipe.SAdd(c, dirtyStateKey, fmt.Sprintf("%s:%s", video_id, curruserName))
	}

	// Queue the increment and capture the command reference to read the result later
	incrCmd := pipe.HIncrBy(c, counterKey, "likes", int64(incrCount))

	// Fire all 3 commands in ONE single network trip!
	_, err := pipe.Exec(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Redis pipeline failed"})
		return
	}

	// 4. Get the result of the increment operation from the pipeline
	newCount := incrCmd.Val()

	// preventing race condition by increamenting the counter blindly before checking the db
	if newCount == 1 || newCount == -1 {

		err := h.redis.Expire(c, counterKey, 60)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to set expiration for counter. Memory Leak can occur."})
			return
		}

		videoInfo, err := h.db.GetVideoDetails(c, video_id)
		if err != nil {
			h.redis.Remove(c, counterKey)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get likes from db to update the like counter"})
			return
		}

		dbLikes := videoInfo.Likes

		_, err = h.redis.IncrementHashField(c, counterKey, "likes", dbLikes)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update the counter"})
			return
		}

	}

	c.Status(http.StatusNoContent)

}
