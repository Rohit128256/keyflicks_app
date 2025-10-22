package routes

import (
	"keyflicks_app/internals/handlers"

	"github.com/gin-gonic/gin"
)

func SetupStreamingRoutes(router *gin.Engine, streamHandler *handlers.StreamHandler) {
	streamRoutes := router.Group("/api")
	{
		streamRoutes.POST("/generate-upload-url/:filename", streamHandler.Generate_upload_url)
		streamRoutes.GET("/stream-status/:upload_id", streamHandler.Get_status)
		streamRoutes.POST("/s3-webhook", streamHandler.Handle_s3_event)
		streamRoutes.GET("/playlist/:video_id/:resolution_path", streamHandler.Sign_segments)
		streamRoutes.GET("/master/:video_id", streamHandler.Modified_master)
		streamRoutes.GET("/status/:upload_id", streamHandler.Stream_status)
	}
}
