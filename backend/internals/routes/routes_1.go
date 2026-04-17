package routes

import (
	"keyflicks_app/internals/handlers"

	"github.com/gin-gonic/gin"
)

func SetupStreamingRoutes(
	router *gin.Engine,
	streamHandler *handlers.StreamHandler,
	authHandler *handlers.AuthHandler,
	eventHandler *handlers.EventHandler,
	authMiddleware gin.HandlerFunc,
) {
	api := router.Group("/api")

	{
		// webhook handler
		api.POST("/s3-webhook", streamHandler.Handle_s3_event)

		// public auth apis
		api.POST("/register", authHandler.UserRegister)
		api.POST("/login", authHandler.UserLogin)

		//protected by cookie
		api.GET("/refresh-token", authHandler.GetNewAccessToken)

		protected := api.Group("")
		protected.Use(authMiddleware)

		{

			// Streaming & Video Management
			protected.POST("/generate-upload-url", streamHandler.Generate_upload_url)
			protected.GET("/stream-status", streamHandler.Stream_status)
			protected.GET("/playlist/:video_id/:resolution_path", streamHandler.Sign_segments)
			protected.GET("/master/:video_id", streamHandler.Modified_master)
			protected.GET("/status/:video_id", streamHandler.Get_status)
			protected.GET("/my-videos", streamHandler.Get_uploaded_videos)
			protected.GET("/get-videos", streamHandler.Get_uploaded_videos_by_user)
			protected.DELETE("/video/:video_id", streamHandler.Delete_video)
			protected.POST("/stream-ack", streamHandler.DeleteSSEStream)

			// Interactions (Likes & Comments)
			protected.POST("/like", eventHandler.ToggleLike)
			protected.GET("/likes", eventHandler.Getlikes)
			protected.POST("/comment", eventHandler.PostComment)
			protected.GET("/getcommentnums", eventHandler.GetCommentsCount)
			protected.GET("/comments", eventHandler.GetComments)
			protected.DELETE("/delcomment", eventHandler.DeleteComment)

			// User Information Endpoints
			protected.GET("/profile/me", authHandler.GetOwnDetails)
			protected.GET("/profile/:username", authHandler.GetUserDetails)

			// profile updating handlers
			protected.PUT("/profile/details", authHandler.UpdateProfileDetails)
			protected.PUT("/profile/picture", authHandler.UploadProfilePicture)

			//logout
			protected.POST("/logout", authHandler.UserLogout)
		}
	}
}
