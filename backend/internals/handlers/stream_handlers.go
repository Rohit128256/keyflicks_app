package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"keyflicks_app/internals/cache"
	"keyflicks_app/internals/celery"
	"keyflicks_app/internals/s3_store"
	"keyflicks_app/internals/signature"
	"log"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type StreamHandler struct {
	S3               *s3_store.S3Store
	redis            *cache.RedisDB
	celery           *celery.Celery
	uri_secret       string
	pending_bucket   string
	streaming_bucket string
	TTL              int
}

func NewStreamHandler(s3 *s3_store.S3Store, rds *cache.RedisDB, cel *celery.Celery, uri_sec string, pend_bucket string, stream_bucket string, exp int) *StreamHandler {
	return &StreamHandler{
		S3:               s3,
		redis:            rds,
		celery:           cel,
		uri_secret:       uri_sec,
		pending_bucket:   pend_bucket,
		streaming_bucket: stream_bucket,
		TTL:              exp,
	}
}

// presigned put url to upload video..
func (h *StreamHandler) Generate_upload_url(c *gin.Context) {

	filename := c.Param("filename")

	id := uuid.New().String()
	video_id := strings.ReplaceAll(id, "-", "")

	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(filename), "."))
	if ext == "" {
		ext = "mp4"
	}

	s3_key := fmt.Sprintf("pending/%s.%s", video_id, ext)

	content_type := mime.TypeByExtension("." + ext)

	if content_type == "" {
		content_type = "application/octet-stream"
	}

	local_presigned_url, err := h.S3.GeneratePresignedUploadUrl(c, h.pending_bucket, s3_key, content_type)

	if err != nil {
		log.Printf("Error generating upload url %v", err)

		errorMsg := fmt.Sprintf("An unexpected error occurred on the server: %v", err)

		// Return the full message in the JSON response
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": errorMsg,
		})
		return
	}

	proto := c.GetHeader("x-forwarded-proto")
	if proto == "" {
		proto = "http"
	}

	host := c.GetHeader("host")

	if host == "" {
		c.JSON(http.StatusOK, gin.H{
			"presigned_url": local_presigned_url,
			"video_id":      video_id,
			"s3_key":        s3_key,
		})
		return
	}

	newBaseURL := fmt.Sprintf("%s://%s", proto, host)

	public_presigned_url := strings.Replace(local_presigned_url, "http://localhost:9000", newBaseURL, -1)

	c.JSON(http.StatusOK, gin.H{
		"presigned_url": public_presigned_url,
		"video_id":      video_id,
		"s3_key":        s3_key,
	})

}

// webhook handler
func (h *StreamHandler) Handle_s3_event(c *gin.Context) {
	var jsonData map[string]interface{}

	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid webhook payload"})
		return
	}

	// 1. Get "Records" as a slice of interfaces
	records, ok := jsonData["Records"].([]interface{})
	if !ok || len(records) == 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Malformed S3 event: 'Records' array is missing or empty"})
		return
	}

	// 2. Get the first record as a map
	record, ok := records[0].(map[string]interface{})
	if !ok {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Malformed S3 event: first record is not a valid object"})
		return
	}

	// 3. Navigate down to the object key
	s3Data, _ := record["s3"].(map[string]interface{})
	objectData, _ := s3Data["object"].(map[string]interface{})
	encodedS3Key, _ := objectData["key"].(string)

	// URL-decode the key
	s3Key, err := url.QueryUnescape(encodedS3Key)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Malformed S3 event: object key is not properly URL-encoded"})
		return
	}

	if s3Key == "" {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Malformed S3 event: missing object key"})
		return
	}

	// Extracting the upload_id from the key: "pending/UPLOAD_ID.mp4"
	parts := strings.Split(s3Key, "/")
	if len(parts) < 2 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Invalid S3 key format"})
		return
	}
	filename := parts[1]
	uploadID := strings.Split(filename, ".")[0]

	err = h.celery.DispatchVideoTranscodeTask(c.Request.Context(), uploadID, s3Key)
	if err != nil {
		log.Printf("CRITICAL: Failed to dispatch Celery task for upload %s: %v", uploadID, err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Failed to start video processing job"})
		return
	}

	log.Printf("Successfully dispatched transcoding job for upload_id: %s, s3_key: %s", uploadID, s3Key)

	c.Status(http.StatusOK)

}

// stream status sse handler
func (h *StreamHandler) Get_status(c *gin.Context) {
	upload_id := c.Param("upload_id")

	if upload_id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "upload_id is required"})
		return
	}

	// Setting the headers needed for Server-Sent Events
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Access-Control-Allow-Origin", "*")

	channel := fmt.Sprintf("job_status_%s", upload_id)

	ctx := c.Request.Context()
	pubsub := h.redis.Subscribe(ctx, channel)
	// Ensure the subscription is closed when the handler exits
	defer pubsub.Close()

	// Use a channel to receive messages from Redis
	redisChan := pubsub.Channel()

	c.Stream(func(w io.Writer) bool {
		select {
		case msg := <-redisChan:
			// A message was received from Redis
			status := msg.Payload
			log.Printf("SSE: Got status '%s' for upload %s", status, upload_id)

			// The nested JSON structure from your Python code
			type sseInnerData struct {
				Status string `json:"status"`
			}
			type sseOuterData struct {
				Data sseInnerData `json:"data"`
			}

			// Create and marshal the data to JSON
			ssePayload := sseOuterData{Data: sseInnerData{Status: status}}
			jsonBytes, _ := json.Marshal(ssePayload)

			// Write the SSE-formatted message to the client
			// The format is "data: <json-payload>\n\n"
			fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))

			// Flush the writer to ensure the message is sent immediately
			c.Writer.Flush()

			// If the job is done, close the connection
			if status == "ready" || status == "failed" {
				log.Printf("SSE: Closing connection for upload %s", upload_id)
				return false // false = close stream
			}

			// Keep the connection open for the next message
			return true // true = continue stream

		case <-ctx.Done():
			// The client has disconnected
			log.Printf("SSE: Client disconnected for upload %s", upload_id)
			return false // false = close stream
		}
	})
}

// handler for sigining playlist...
func (h *StreamHandler) Sign_segments(c *gin.Context) {
	videoID := c.Param("video_id")
	resolutionPath := c.Param("resolution_path")

	const REFRESH_THRESHOLD_SECONDS = 25 * 60
	cacheTTLSeconds := h.TTL + 300

	cacheKey := fmt.Sprintf("playlist:%s:%s", videoID, resolutionPath)
	now := time.Now().Unix()

	type cacheData struct {
		Playlist  string `json:"playlist"`
		ExpiresAt int64  `json:"expires_at"`
	}

	// Try cache
	if h.redis != nil {
		if cachedStr, err := h.redis.Get(c.Request.Context(), cacheKey); err == nil && cachedStr != "" {
			var cd cacheData
			if err := json.Unmarshal([]byte(cachedStr), &cd); err == nil {
				remaining := cd.ExpiresAt - now
				if remaining > int64(REFRESH_THRESHOLD_SECONDS) {
					// Cache HIT and still fresh
					c.Data(http.StatusOK, "application/vnd.apple.mpegurl", []byte(cd.Playlist))
					return
				}
				// Cache HIT but stale: fall through to regenerate
			}
		}
	}

	// Cache MISS or stale: fetch original playlist from S3
	s3Key := fmt.Sprintf("videos/%s/%s/playlist.m3u8", videoID, resolutionPath)
	body, err := h.S3.GetObject(c.Request.Context(), h.streaming_bucket, s3Key)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Failed to fetch master playlist: %v", err)})
		return
	}
	defer body.Close()

	playlistBytes, err := io.ReadAll(body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Failed to read master playlist: %v", err)})
		return
	}
	playlistContent := string(playlistBytes)

	// Rewrite with fresh signatures
	expires := now + int64(h.TTL)
	rewritten := signature.RewritePlaylist(playlistContent, videoID, resolutionPath, expires, h.uri_secret)

	// Background cache update (decoupled from request context)
	go func(data cacheData) {
		bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		b, err := json.Marshal(data)
		if err != nil {
			return
		}
		if h.redis != nil {
			_ = h.redis.Set(bgCtx, cacheKey, string(b), cacheTTLSeconds)
		}
	}(cacheData{
		Playlist:  rewritten,
		ExpiresAt: expires,
	})

	// Respond with the rewritten playlist
	c.Data(http.StatusOK, "application/vnd.apple.mpegurl", []byte(rewritten))
}

// handler to get the master playlist
func (h *StreamHandler) Modified_master(c *gin.Context) {

	videoId := c.Param("video_id")

	cache_key := fmt.Sprintf("master:%s", videoId)

	type cacheData struct {
		Playlist string `json:"playlist"`
	}

	// Try cache
	if h.redis != nil {
		if cachedStr, err := h.redis.Get(c.Request.Context(), cache_key); err == nil && cachedStr != "" {
			var cd cacheData
			if err := json.Unmarshal([]byte(cachedStr), &cd); err == nil {
				c.Data(http.StatusOK, "application/vnd.apple.mpegurl", []byte(cd.Playlist))
				return
			}
			// Cache HIT but stale: fall through to regenerate
		}
	}

	//cache miss happened
	s3Key := fmt.Sprintf("videos/%s/master.m3u8", videoId)
	body, err := h.S3.GetObject(c.Request.Context(), h.streaming_bucket, s3Key)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Failed to fetch master playlist: %v", err)})
		return
	}
	defer body.Close()

	playlistBytes, err := io.ReadAll(body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"detail": fmt.Sprintf("Failed to read master playlist: %v", err)})
		return
	}
	playlistContent := string(playlistBytes)

	rewritten_playlist := signature.RewriteMasterPlaylist(playlistContent, videoId)

	// Background cache update (decoupled from request context)
	go func(data cacheData) {
		bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		b, err := json.Marshal(data)
		if err != nil {
			return
		}
		if h.redis != nil {
			_ = h.redis.Set(bgCtx, cache_key, string(b), h.TTL)
		}
	}(cacheData{
		Playlist: rewritten_playlist,
	})

	// Respond with the rewritten playlist
	c.Data(http.StatusOK, "application/vnd.apple.mpegurl", []byte(rewritten_playlist))

}

// handler function to see the status of video using video id
func (h *StreamHandler) Stream_status(c *gin.Context) {
	uploadID := c.Param("upload_id")
	cacheKey := fmt.Sprintf("upload_status:%s", uploadID)

	// The struct for both the cache and the final API response.
	// Using `int` for resolutions to allow for proper sorting.
	type responseData struct {
		UploadID             string `json:"upload_id"`
		Status               string `json:"status"`
		AvailableResolutions []int  `json:"available_resolutions"`
	}

	// 1. Try to fetch from the cache first.
	if h.redis != nil {
		if cachedStr, err := h.redis.Get(c.Request.Context(), cacheKey); err == nil && cachedStr != "" {
			var data responseData
			if err := json.Unmarshal([]byte(cachedStr), &data); err == nil {
				log.Printf("Cache HIT for upload_id: %s", uploadID)
				c.JSON(http.StatusOK, data)
				return
			}
		}
	}

	log.Printf("Cache MISS for upload_id: %s", uploadID)

	// 2. Cache MISS: Query S3 for the list of objects.
	// The trailing slash is important for listing objects within the "folder".
	prefix := fmt.Sprintf("videos/%s/", uploadID)

	objects, err := h.S3.ListObjects(c.Request.Context(), h.streaming_bucket, prefix)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Failed to query upload status"})
		return
	}

	if len(objects) == 0 {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "Upload ID not found"})
		return
	}

	// 3. Determine status based on the files found.
	// A map is the Go equivalent of Python's set for efficient lookups.
	keys := make(map[string]bool)
	for _, obj := range objects {
		// Equivalent to Python's .removeprefix()
		keys[strings.TrimPrefix(*obj.Key, prefix)] = true
	}

	status := "processing"
	resolutions := []int{}

	if keys["master.m3u8"] {
		status = "ready"
		for k := range keys {
			// e.g., "360p/playlist.m3u8"
			if strings.HasSuffix(k, "playlist.m3u8") && strings.Contains(k, "/") {
				resStr := strings.Split(k, "/")[0] // "360p"
				if strings.HasSuffix(resStr, "p") {
					// Convert "360p" -> "360" -> 360 (int)
					if resInt, err := strconv.Atoi(strings.TrimSuffix(resStr, "p")); err == nil {
						resolutions = append(resolutions, resInt)
					}
				}
			}
		}
	}

	// 4. Construct the final response.
	sort.Ints(resolutions) // Equivalent to Python's sorted()
	finalResponse := responseData{
		UploadID:             uploadID,
		Status:               status,
		AvailableResolutions: resolutions,
	}

	// 5. Update the cache in the background.
	// This is the Go equivalent of background_tasks.add_task.
	go func() {
		jsonData, err := json.Marshal(finalResponse)
		if err != nil {
			log.Printf("Background cache update failed (marshal): %v", err)
			return
		}
		// A background task should have its own context that isn't tied to the request.
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := h.redis.Set(bgCtx, cacheKey, string(jsonData), h.TTL); err != nil {
			log.Printf("Background cache update failed (set): %v", err)
		} else {
			log.Printf("Background cache update SUCCESS for upload_id: %s", uploadID)
		}
	}()

	// 6. Return the final response to the client.
	c.JSON(http.StatusOK, finalResponse)

}
