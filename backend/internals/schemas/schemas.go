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
