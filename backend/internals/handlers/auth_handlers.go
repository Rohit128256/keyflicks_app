package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"keyflicks_app/internals/auth"
	"keyflicks_app/internals/cache"
	database "keyflicks_app/internals/db"
	"keyflicks_app/internals/s3_store"
	"keyflicks_app/internals/schemas"
	"keyflicks_app/internals/security"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gin-gonic/gin"
)

type AuthHandler struct {
	store          *database.DbStore
	s3             *s3_store.S3Store
	jwt            *auth.Jwt
	redis          *cache.RedisDB
	profile_bucket string
}

func NewAuthHandler(store *database.DbStore, s3 *s3_store.S3Store, jwt *auth.Jwt, redis *cache.RedisDB, prof_bucket string) *AuthHandler {
	return &AuthHandler{
		store:          store,
		jwt:            jwt,
		s3:             s3,
		redis:          redis,
		profile_bucket: prof_bucket,
	}
}

// register handler
func (h *AuthHandler) UserRegister(c *gin.Context) {
	var newUser schemas.UserCreateDB

	username := c.PostForm("username")
	firstname := c.PostForm("firstname")
	lastname := c.PostForm("lastname")
	bio := c.PostForm("bio")
	email := c.PostForm("email")
	password := c.PostForm("password")
	dobString := c.PostForm("dob")

	// checking if something's missing
	if username == "" || email == "" || password == "" || dobString == "" || firstname == "" || lastname == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Required field missing!!"})
		return
	}

	//check if email format is correct or not
	if !security.IsEmailLikelyValid(email) {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Email type is invalid"})
		return
	}

	//convert dobString to actual format
	dob, err := time.Parse(time.RFC3339, dobString)

	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Dob type is invalid"})
		return
	}

	// hash the password
	hashPass, err := security.HashPassword(password)

	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// assign the values to the newUser struct
	newUser.Username = username
	newUser.FirstName = firstname
	newUser.LastName = lastname
	newUser.Bio = bio
	newUser.Email = email
	newUser.HashedPassword = hashPass
	newUser.DOB = dob

	// register user to the database
	userFromDB, err := h.store.CreateNewUser(c.Request.Context(), &newUser)

	if err != nil {
		if errors.Is(err, database.ErrEmailExists) || errors.Is(err, database.ErrUsernameExists) {
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": err.Error()})
		} else {
			log.Printf("Internal server error in RegisterUser: %v", err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "An unexpected error occurred. Please try again."})
		}
		return
	}

	// get the image from request ans upload it in background
	fileHeader, err := c.FormFile("profile_pic")

	if err == nil {
		file, err := fileHeader.Open()
		if err != nil {
			log.Printf("couldnot open the file : %v", err)
		} else {
			user_id := userFromDB.ID.String()
			s3Key := fmt.Sprintf("%s/profileImage", user_id)
			contentType := fileHeader.Header.Get("Content-Type")

			fileBytes, err := io.ReadAll(file)
			file.Close()

			if err != nil {
				log.Printf("failed to read the file for id %s : %v", user_id, err)
			} else {
				go func() {
					bgCtx := context.Background()

					//creating io.Reader from your byte slice
					reader := bytes.NewReader(fileBytes)

					_, err := h.s3.PutObject(bgCtx, h.profile_bucket, s3Key, reader, contentType)

					if err != nil {
						log.Printf("failed to upload profile pic for user %s in background: %v", user_id, err)
					} else {
						log.Printf("Successfully uploaded profile pic for user %s in background", user_id)
					}
				}()
			}

		}
	}

	token, err := h.jwt.Encode(userFromDB.Username)
	if err != nil {
		log.Printf("Failed to generate access token for user %s: %v", userFromDB.Username, err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Could not log in user after registration."})
		return
	}

	refreshToken, err := h.jwt.GenerateRefreshToken(userFromDB.Username)
	if err != nil {
		log.Printf("Failed to generate refreshtoken token for user %s: %v", userFromDB.Username, err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Could not log in user after registration."})
		return
	}

	c.SetCookie(
		"refresh_token",      // cookie name
		refreshToken,         // cookie value
		3600*24*60,           // max age in seconds (60 days)
		"/api/refresh-token", // path
		"",                   // domain (frontend's domain in production)
		true,                 // secure (true = only send over HTTPS)
		true,                 // httpOnly (true = JavaScript can't read it)
	)

	c.JSON(http.StatusCreated, gin.H{
		"status":       "User created successfully.",
		"user":         userFromDB.Username,
		"access_token": token,
		"token_type":   "Bearer",
	})

}

// login handler
func (h *AuthHandler) UserLogin(c *gin.Context) {
	var userIn schemas.UserLoginIn
	if err := c.ShouldBind(&userIn); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if userIn.Email == "" || userIn.Password == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "email or password can't be empty!"})
		return
	}

	//check if email format is correct or not
	if !security.IsEmailLikelyValid(userIn.Email) {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Email type is invalid"})
		return
	}

	userFromDB, err := h.store.GetUserByEmail(c.Request.Context(), userIn.Email)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "User not found"})
		} else {
			// this is a server error
			log.Printf("Database error in auth middleware: %v", err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Internal server error of pgsql"})
		}
		return
	}

	err = security.VerifyPassword(userFromDB.HashedPassword, userIn.Password)

	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Password is incorrect"})
	}

	token, err := h.jwt.Encode(userFromDB.Username)
	if err != nil {
		log.Printf("Failed to generate access token for user %s: %v", userFromDB.Username, err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Could not log in user after registration."})
		return
	}

	refreshToken, err := h.jwt.GenerateRefreshToken(userFromDB.Username)
	if err != nil {
		log.Printf("Failed to generate refreshtoken token for user %s: %v", userFromDB.Username, err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Could not log in user after registration."})
		return
	}

	c.SetCookie(
		"refresh_token",      // cookie name
		refreshToken,         // cookie value
		3600*24*60,           // max age in seconds (60 days)
		"/api/refresh-token", // path (only send to your auth routes)
		"",                   // domain (frontend's domain in production)
		true,                 // secure (true = only send over HTTPS)
		true,                 // httpOnly (true = JavaScript can't read it)
	)

	c.JSON(http.StatusCreated, gin.H{
		"access_token": token,
		"token_type":   "Bearer",
	})

}

// accesstoken token handler
func (h *AuthHandler) GetNewAccessToken(c *gin.Context) {
	refreshToken, err := c.Cookie("refresh_token")
	if err != nil || refreshToken == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Refresh token is missing or expired"})
		return
	}

	// validation
	payload, err := h.jwt.Decode(refreshToken)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired refresh token"})
		return
	}

	username, ok := payload["sub"].(string)
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token payload"})
		return
	}

	newAccessToken, err := h.jwt.Encode(username)
	if err != nil {
		log.Printf("Failed to generate access token for user %s: %v", username, err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Could not generate new access token."})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"access_token": newAccessToken,
		"token_type":   "Bearer",
	})
}

func (h *AuthHandler) GetOwnDetails(c *gin.Context) {
	// extract the user set by the AuthMiddleware
	userInter, exists := c.Get("currentUser")
	if !exists {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}

	currUser := userInter.(*schemas.UserInDB)

	// Return only the requested fields
	c.JSON(http.StatusOK, gin.H{
		"userid":          currUser.ID,
		"username":        currUser.Username,
		"firstname":       currUser.FirstName,
		"lastname":        currUser.LastName,
		"bio":             currUser.Bio,
		"email":           currUser.Email,
		"videos_uploaded": currUser.UploadedVideos,
		"dob":             currUser.DOB,
	})
}

func (h *AuthHandler) GetUserDetails(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "username parameter is required"})
		return
	}

	cacheKey := fmt.Sprintf("UserProfile:%s", username)

	// check Cache first
	if cachedData, err := h.redis.Get(c.Request.Context(), cacheKey); err == nil && cachedData != "" {
		var userData map[string]any
		if err := json.Unmarshal([]byte(cachedData), &userData); err == nil {
			// Cache hit: Return immediately
			c.JSON(http.StatusOK, userData)
			return
		}
	}

	// 2. Cache Miss: Fetch from database
	userFromDB, err := h.store.GetUserByName(c.Request.Context(), username)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "User not found"})
		} else {
			log.Printf("Database error fetching user %s: %v", username, err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		}
		return
	}

	// Build the response object with specific fields
	response := gin.H{
		"userid":          userFromDB.ID,
		"username":        userFromDB.Username,
		"firstname":       userFromDB.FirstName,
		"lastname":        userFromDB.LastName,
		"bio":             userFromDB.Bio,
		"videos_uploaded": userFromDB.UploadedVideos,
		"email":           userFromDB.Email,
		"dob":             userFromDB.DOB,
	}

	// 3. Set Cache in the background (Non-blocking)
	go func(data map[string]any) {
		bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		b, err := json.Marshal(data)
		if err != nil {
			log.Printf("Failed to marshal user data for cache: %v", err)
			return
		}

		if h.redis != nil {
			// Cache the profile for 15 minutes (900 seconds)
			err = h.redis.Set(bgCtx, cacheKey, string(b), 900)
			if err != nil {
				log.Printf("Failed to set redis cache for user profile %s: %v", username, err)
			}
		}
	}(response)

	c.JSON(http.StatusOK, response)
}

// UploadProfilePicture allows an authenticated user to update their profile picture
func (h *AuthHandler) UploadProfilePicture(c *gin.Context) {
	// get current user
	user, exists := c.Get("currentUser")
	if !exists {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	currUser := user.(*schemas.UserInDB)
	userID := currUser.ID.String()

	// get the file
	fileHeader, err := c.FormFile("profile_pic")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile picture file is required"})
		return
	}

	// opening thje file
	file, err := fileHeader.Open()
	if err != nil {
		log.Printf("Could not open profile pic file for user %s: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process the uploaded file"})
		return
	}
	defer file.Close()

	// reading and getting the file bytes
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		log.Printf("Failed to read profile pic file for user %s: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read the uploaded file"})
		return
	}

	// uploading to s3 bucket
	s3Key := fmt.Sprintf("%s/profileImage", userID)
	contentType := fileHeader.Header.Get("Content-Type")
	reader := bytes.NewReader(fileBytes)
	_, err = h.s3.PutObject(c.Request.Context(), h.profile_bucket, s3Key, reader, contentType)

	if err != nil {
		log.Printf("Failed to upload profile pic to S3 for user %s: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save profile picture"})
		return
	}

	// return success status
	c.JSON(http.StatusOK, gin.H{
		"message": "Profile picture updated successfully",
		"s3_key":  s3Key,
	})
}

// update user info
func (h *AuthHandler) UpdateProfileDetails(c *gin.Context) {
	// 1. Get current logged-in user
	user, exists := c.Get("currentUser")
	if !exists {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	currUser := user.(*schemas.UserInDB)
	userID := currUser.ID.String()

	// 2. Define expected JSON payload (All fields are now optional)
	var reqBody struct {
		Email     string `json:"email"`
		Username  string `json:"username"`
		DOB       string `json:"dob"`
		FirstName string `json:"firstname"`
		LastName  string `json:"lastname"`
		Bio       string `json:"bio"`
	}

	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload"})
		return
	}

	if reqBody.Email == "" && reqBody.Username == "" && reqBody.DOB == "" && reqBody.FirstName == "" && reqBody.LastName == "" && reqBody.Bio == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No fields provided for update"})
		return
	}

	// 3. Merge request data with existing user data (Partial Update Logic)
	emailToUpdate := currUser.Email
	if reqBody.Email != "" {
		if !security.IsEmailLikelyValid(reqBody.Email) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid email format"})
			return
		}
		emailToUpdate = reqBody.Email
	}

	usernameToUpdate := currUser.Username
	if reqBody.Username != "" {
		usernameToUpdate = reqBody.Username
	}

	dobToUpdate := currUser.DOB
	if reqBody.DOB != "" {
		parsedDob, err := time.Parse(time.RFC3339, reqBody.DOB)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid DOB format. Expected valid timestamp."})
			return
		}
		dobToUpdate = parsedDob
	}

	firstnameToUpdate := currUser.FirstName
	if reqBody.FirstName != "" {
		firstnameToUpdate = reqBody.FirstName
	}

	lastnameToUpdate := currUser.LastName
	if reqBody.LastName != "" {
		lastnameToUpdate = reqBody.LastName
	}

	bioToUpdate := currUser.Bio
	if reqBody.Bio != "" {
		bioToUpdate = reqBody.Bio
	}

	// 4. Update Database
	err := h.store.UpdateUserDetails(c.Request.Context(), userID, emailToUpdate, usernameToUpdate, dobToUpdate, firstnameToUpdate, lastnameToUpdate, bioToUpdate)
	if err != nil {
		// Handle cases where the requested email or username is already taken by someone else
		if errors.Is(err, database.ErrEmailExists) || errors.Is(err, database.ErrUsernameExists) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}

		log.Printf("Database error updating profile for user %s: %v", userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update profile details"})
		return
	}

	// 5. Invalidate stale caches so the next request gets fresh data
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if h.redis != nil {
			// Invalidate both old and new username keys (handles username change)
			_ = h.redis.Remove(bgCtx,
				fmt.Sprintf("JwtAuth:%s", currUser.Username),     // old username auth cache
				fmt.Sprintf("JwtAuth:%s", usernameToUpdate),      // new username auth cache
				fmt.Sprintf("UserProfile:%s", currUser.Username), // old username profile cache
				fmt.Sprintf("UserProfile:%s", usernameToUpdate),  // new username profile cache
			)
		}
	}()

	// 6. If username changed, re-issue tokens since JWT encodes the username as `sub`
	usernameChanged := usernameToUpdate != currUser.Username
	response := gin.H{
		"message":  "Profile updated successfully",
		"username": usernameToUpdate,
		"email":    emailToUpdate,
	}

	if usernameChanged {
		token, err := h.jwt.Encode(usernameToUpdate)
		if err != nil {
			log.Printf("Failed to generate new access token after username change for user %s: %v", userID, err)
			// Profile is already updated, so still return success but without new tokens
			c.JSON(http.StatusOK, response)
			return
		}

		refreshToken, err := h.jwt.GenerateRefreshToken(usernameToUpdate)
		if err != nil {
			log.Printf("Failed to generate new refresh token after username change for user %s: %v", userID, err)
			c.JSON(http.StatusOK, response)
			return
		}

		c.SetCookie(
			"refresh_token",
			refreshToken,
			3600*24*60,
			"/api/refresh-token",
			"",
			true,
			true,
		)

		response["access_token"] = token
		response["token_type"] = "Bearer"
	}

	c.JSON(http.StatusOK, response)
}

func (h *AuthHandler) UserLogout(c *gin.Context) {
	c.SetCookie("refresh_token", "", -1, "/api/refresh-token", "", false, true)
	c.JSON(200, gin.H{
		"message": "Successfully logged out",
	})
}
