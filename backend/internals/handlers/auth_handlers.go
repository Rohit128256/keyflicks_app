package handlers

import (
	"bytes"
	"context"
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

func NewAuthHandler(store *database.DbStore, jwt *auth.Jwt, redis *cache.RedisDB, prof_bucket string) *AuthHandler {
	return &AuthHandler{
		store:          store,
		jwt:            jwt,
		redis:          redis,
		profile_bucket: prof_bucket,
	}
}

// register handler
func (h *AuthHandler) UserRegister(c *gin.Context) {
	var newUser schemas.UserCreateDB

	username := c.PostForm("username")
	email := c.PostForm("email")
	password := c.PostForm("password")
	dobString := c.PostForm("dob")

	// checking if something's missing
	if username == "" || email == "" || password == "" || dobString == "" {
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
		"refresh_token", // cookie name
		refreshToken,    // cookie value
		3600*24*60,      // max age in seconds (60 days)
		"/",             // path
		"",              // domain (frontend's domain in production)
		true,            // secure (true = only send over HTTPS)
		true,            // httpOnly (true = JavaScript can't read it)
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
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
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
		"refresh_token", // cookie name
		refreshToken,    // cookie value
		3600*24*60,      // max age in seconds (60 days)
		"/",             // path (only send to your auth routes)
		"",              // domain (frontend's domain in production)
		true,            // secure (true = only send over HTTPS)
		true,            // httpOnly (true = JavaScript can't read it)
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

	c.JSON(http.StatusOK, gin.H{
		"access_token": newAccessToken,
		"token_type":   "Bearer",
	})
}
