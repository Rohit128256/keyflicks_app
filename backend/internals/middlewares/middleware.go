package middlewares

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"keyflicks_app/internals/auth"
	"keyflicks_app/internals/cache"
	database "keyflicks_app/internals/db"
	"keyflicks_app/internals/schemas"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

func AuthMiddleware(store *database.DbStore, redis *cache.RedisDB, jwt *auth.Jwt) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error ": "Authorization header is Required"})
			return
		}

		headerParts := strings.Split(authHeader, " ")

		if len(headerParts) != 2 || headerParts[0] != "Bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization header format must be Bearer {token}"})
			return
		}

		token := headerParts[1]

		payload, err := jwt.Decode(token)

		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}

		currUserName, ok := payload["sub"].(string)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token payload"})
			return
		}

		// cache check and database check for user
		cache_key := fmt.Sprintf("JwtAuth:%s", currUserName)
		if cached_user, err := redis.Get(c.Request.Context(), cache_key); err == nil && cached_user != "" {
			// cache hit
			var CachedData schemas.UserInDB
			if err := json.Unmarshal([]byte(cached_user), &CachedData); err == nil {
				c.Set("currentUser", &CachedData)
				c.Next()
				return
			}
		}

		// cache miss db check
		CurrUser, err := store.GetUserByName(c.Request.Context(), currUserName)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "User not found"})
			} else {
				// this is a server error
				log.Printf("Database error in auth middleware: %v", err)
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
			}
			return
		}

		CurrUser.HashedPassword = ""

		// Background cache update (decoupled from request context)
		go func(data schemas.UserInDB) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			b, err := json.Marshal(data)
			if err != nil {
				return
			}
			if redis != nil {
				_ = redis.Set(bgCtx, cache_key, string(b), 9000)
			}
		}(*CurrUser)

		c.Set("currentUser", CurrUser)

		c.Next()
	}
}
