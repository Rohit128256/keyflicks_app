package schemas

import (
	"time"

	"github.com/google/uuid"
)

type UserLoginIn struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type UserCreateDB struct {
	Email          string
	HashedPassword string
	Username       string
	DOB            time.Time
}

type UserInDB struct {
	ID             uuid.UUID `json:"id" db:"id"`
	Email          string    `json:"email" db:"email"`
	HashedPassword string    `json:"-" db:"hashed_password"` // Note the json:"-" tag
	Username       string    `json:"username" db:"username"`
	DOB            time.Time `json:"dob" db:"dob"`
	CreatedAt      time.Time `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time `json:"updated_at" db:"updated_at"`
}

type VideoUploadInfo struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Filename    string `json:"file_name"`
}

type VideoInDb struct {
	ID          string    `json:"id" db:"id"`
	Title       string    `json:"title" db:"title"`
	Description string    `json:"description" db:"description"`
	Likes       int64     `json:"likes" db:"like_count"`
	Comments    int64     `json:"comments" db:"comment_count"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

type GetlikeState struct {
	VideoLikes    int64 `json:"videoLikes"`
	CurrUserLiked bool  `json:"currUserLiked"`
}

type VideoComment struct {
	ID          string `json:"video_id"`
	PID         string `json:"parent_id"`
	CommentText string `json:"text"`
}

type CommentAuthor struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
}

type CommentResponse struct {
	ID          string        `json:"id"`
	ParentID    *string       `json:"parentId"` // Pointer so it can serialize to null
	Author      CommentAuthor `json:"author"`
	Text        string        `json:"text"`
	ReplyCounts int64         `json:"replyCounts"`
	CreatedAt   time.Time     `json:"createdAt"`
}

type PaginatedComments struct {
	Comments   []CommentResponse `json:"comments"`
	NextCursor *time.Time        `json:"nextCursor"` // Pointer for null if no more pages
}

type ResponseVideoData struct {
	UploadID    string `json:"upload_id"`
	Status      string `json:"status"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Likes       int64  `json:"like_counts,omitempty"` // Changed to match your schema needs
	CreatedAt   string `json:"created_at,omitempty"`
}

type DeleteCommentRequest struct {
	CommentID string `json:"comment_id" binding:"required"`
	VideoID   string `json:"video_id" binding:"required"`
}
