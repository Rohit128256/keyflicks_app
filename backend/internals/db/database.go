package database

import (
	"context"
	"errors"
	"fmt"
	"keyflicks_app/internals/schemas"
	"strings"
	"time"

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

func (s *DbStore) GetLikeState(ctx context.Context, video_id string, user_id string) (*schemas.GetlikeState, error) {
	var LikeState schemas.GetlikeState

	query := `
			SELECT 
				v.like_count, 
				EXISTS(
					SELECT 1 FROM video_likes vl 
					WHERE vl.video_id = v.id AND vl.user_id = $2 AND vl.type = 'like'
				)
			FROM videos v 
			WHERE v.id = $1;
	`

	err := s.db.QueryRow(ctx, query, video_id, user_id).Scan(LikeState.VideoLikes, LikeState.CurrUserLiked)

	if err != nil {
		return nil, err
	}

	return &LikeState, nil

}

func (s *DbStore) GetComments(ctx context.Context, videoID string, parentID *string, cursor *time.Time, limit int) ([]schemas.CommentResponse, error) {
	var query strings.Builder

	// Pre-allocate slice capacity to reduce memory re-allocations during high traffic
	args := make([]any, 0, 4)

	// 1. Base Query
	query.WriteString(`
		SELECT 
			c.id, c.parent_id, c.text, c.reply_counts, c.created_at, 
			u.id AS user_id, u.username
		FROM comments c
		JOIN users u ON c.user_id = u.id
		WHERE c.video_id = $1
	`)
	args = append(args, videoID)
	argID := 2 //video_id and limit is confirmed that's why bydefault 2

	// 2. Dynamic Parent ID (Fixes the "OR IS NULL" planner issue)
	if parentID == nil {
		query.WriteString(` AND c.parent_id IS NULL`)
	} else {
		fmt.Fprintf(&query, ` AND c.parent_id = $%d`, argID)
		args = append(args, *parentID)
		argID++
	}

	// 3. Dynamic Keyset Pagination (Cursor)
	if cursor != nil {
		fmt.Fprintf(&query, ` AND c.created_at < $%d`, argID)
		args = append(args, *cursor)
		argID++
	}

	// 4. Sort and Limit
	fmt.Fprintf(&query, ` ORDER BY c.created_at DESC LIMIT $%d`, argID)
	args = append(args, limit)

	// 5. Execute Query
	rows, err := s.db.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GetComments query: %w", err)
	}
	defer rows.Close()

	// Pre-allocate to the limit to avoid resizing the array under load
	comments := make([]schemas.CommentResponse, 0, limit)

	for rows.Next() {
		var c schemas.CommentResponse
		var pID *string // pgx natively supports scanning directly into a *string for NULLs

		err := rows.Scan(
			&c.ID, &pID, &c.Text, &c.ReplyCounts, &c.CreatedAt,
			&c.Author.UserID, &c.Author.Username,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan comment row: %w", err)
		}

		c.ParentID = pID
		comments = append(comments, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error in GetComments: %w", err)
	}

	return comments, nil
}

func (s *DbStore) GetUserTopLevelComments(ctx context.Context, videoID string, userID string) ([]schemas.CommentResponse, error) {
	query := `
		SELECT 
			c.id, c.parent_id, c.text, c.reply_counts, c.created_at, 
			u.id AS user_id, u.username
		FROM comments c
		JOIN users u ON c.user_id = u.id
		WHERE c.video_id = $1 
		  AND c.user_id = $2 
		  AND c.parent_id IS NULL
		ORDER BY c.created_at DESC
	`

	rows, err := s.db.Query(ctx, query, videoID, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GetUserTopLevelComments query: %w", err)
	}
	defer rows.Close()

	var userComments []schemas.CommentResponse
	for rows.Next() {
		var c schemas.CommentResponse
		var pID *string

		err := rows.Scan(
			&c.ID, &pID, &c.Text, &c.ReplyCounts, &c.CreatedAt,
			&c.Author.UserID, &c.Author.Username,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user comment row: %w", err)
		}

		c.ParentID = pID
		userComments = append(userComments, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error in GetUserTopLevelComments: %w", err)
	}

	return userComments, nil
}

// GetUserUploadedVideos fetches a paginated list of videos uploaded by a specific user.
func (s *DbStore) GetUserUploadedVideos(ctx context.Context, userID string, cursorTime *time.Time, cursorID *string, limit int) ([]schemas.VideoInDb, error) {
	var query strings.Builder

	// Pre-allocate slice capacity to reduce memory re-allocations
	args := make([]any, 0, 4)

	// base Query
	query.WriteString(`
		SELECT id, title, description, like_count, comment_count, created_at, updated_at
		FROM videos
		WHERE user_id = $1
	`)
	args = append(args, userID)
	argID := 2

	// dynamic keyset pagination (Cursor)
	// Using tuple comparison guarantees strict ordering without in-memory sorting
	if cursorTime != nil && cursorID != nil {
		fmt.Fprintf(&query, ` AND (created_at, id) < ($%d, $%d)`, argID, argID+1)
		args = append(args, *cursorTime, *cursorID)
		argID += 2
	}

	// Sort and Limit
	// Order must match the tuple comparison direction to utilize the index efficiently
	fmt.Fprintf(&query, ` ORDER BY created_at DESC, id DESC LIMIT $%d`, argID)
	args = append(args, limit)

	rows, err := s.db.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GetUserUploadedVideos query: %w", err)
	}
	defer rows.Close()

	videos := make([]schemas.VideoInDb, 0, limit)

	for rows.Next() {
		var v schemas.VideoInDb

		err := rows.Scan(
			&v.ID, &v.Title, &v.Description, &v.Likes, &v.Comments, &v.CreatedAt, &v.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user video row: %w", err)
		}

		videos = append(videos, v)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error in GetUserUploadedVideos: %w", err)
	}

	return videos, nil
}

func (s *DbStore) DeleteVideoByOwner(ctx context.Context, videoID string, userID string) error {
	query := `DELETE FROM videos WHERE id = $1 AND user_id = $2`

	tag, err := s.db.Exec(ctx, query, videoID, userID)
	if err != nil {
		return fmt.Errorf("failed to execute delete query: %w", err)
	}

	// If no rows were affected, the video either doesn't exist or belongs to someone else
	if tag.RowsAffected() == 0 {
		return errors.New("video not found or unauthorized to delete")
	}

	return nil
}

func (s *DbStore) UpdateUserDetails(ctx context.Context, userID string, email string, username string, dob time.Time) error {
	query := `
		UPDATE users 
		SET email = $1, username = $2, dob = $3
		WHERE id = $4
	`

	_, err := s.db.Exec(ctx, query, email, username, dob, userID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// Code 23505 is PostgreSQL's unique_violation error code
			if pgErr.Code == "23505" {
				if strings.Contains(pgErr.Message, "email") {
					return ErrEmailExists
				}
				if strings.Contains(pgErr.Message, "username") {
					return ErrUsernameExists
				}
			}
		}
		return fmt.Errorf("failed to update user details: %w", err)
	}

	return nil
}
