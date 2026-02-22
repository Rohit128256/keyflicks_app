package database

import (
	"context"
	"errors"
	"fmt"
	"keyflicks_app/internals/schemas"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrEmailExists    = errors.New("a user with this email already exists")
	ErrUsernameExists = errors.New("a user with this username already exists")
)

type DbStore struct {
	db *pgxpool.Pool
}

func NewDbStore(pool *pgxpool.Pool) *DbStore {
	return &DbStore{db: pool}
}

// function to create new user in database
func (s *DbStore) CreateNewUser(ctx context.Context, DBInput *schemas.UserCreateDB) (*schemas.UserInDB, error) {
	// our sql quesry to insert users
	sql := `INSERT INTO users (email, hashed_password, username, dob)
			VALUES($1,$2,$3,$4)
			RETURNING id, email, username, dob, created_at, updated_at`

	var newUser schemas.UserInDB
	err := s.db.QueryRow(ctx, sql, DBInput.Email, DBInput.HashedPassword,
		DBInput.Username, DBInput.DOB).Scan(
		&newUser.ID,
		&newUser.Email,
		&newUser.Username,
		&newUser.DOB,
		&newUser.CreatedAt,
		&newUser.UpdatedAt,
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// checking if the error code is '23505' (unique_violation)
			if pgErr.Code == "23505" {
				if strings.Contains(pgErr.Message, "email") {
					return nil, ErrEmailExists
				}
				if strings.Contains(pgErr.Message, "username") {
					return nil, ErrUsernameExists
				}
				return nil, ErrUsernameExists
			}
		}

		return nil, fmt.Errorf("could not create user: %w", err)

	}

	newUser.HashedPassword = DBInput.HashedPassword

	return &newUser, nil
}

// function to get user by username
func (s *DbStore) GetUserByName(ctx context.Context, username string) (*schemas.UserInDB, error) {

	sql := `SELECT id, email, hashed_password, username, dob, created_at, updated_at
			FROM users
			WHERE username = $1`

	var user schemas.UserInDB

	err := s.db.QueryRow(ctx, sql, username).Scan(
		user.ID,
		user.Email,
		user.HashedPassword,
		user.Username,
		user.DOB,
		user.CreatedAt,
		user.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &user, nil
}

// function to get user by email
// function to get user by username
func (s *DbStore) GetUserByEmail(ctx context.Context, email string) (*schemas.UserInDB, error) {

	sql := `SELECT id, email, hashed_password, username, dob, created_at, updated_at
			FROM users
			WHERE email = $1`

	var user schemas.UserInDB

	err := s.db.QueryRow(ctx, sql, email).Scan(
		user.ID,
		user.Email,
		user.HashedPassword,
		user.Username,
		user.DOB,
		user.CreatedAt,
		user.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &user, nil
}

// function to get Video details by video id
func (s *DbStore) GetVideoDetails(ctx context.Context, video_id string) (*schemas.VideoInDb, error) {

	sql := `SELECT id , title , decription , like_count , comment_count , created_at , updated_at
			FROM videos
			WHERE id = $1`

	var video schemas.VideoInDb
	err := s.db.QueryRow(ctx, sql, video_id).Scan(
		video.ID,
		video.Title,
		video.Description,
		video.Likes,
		video.Comments,
		video.CreatedAt,
		video.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &video, nil
}
