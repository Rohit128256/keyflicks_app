package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Jwt struct {
	secret_key string
}

func NewJwt(key string) *Jwt {
	return &Jwt{
		secret_key: key,
	}
}

func (j *Jwt) Encode(sub string) (string, error) {
	claims := jwt.MapClaims{
		"sub": sub,
		"exp": time.Now().Add(30 * time.Minute).Unix(),
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	token, err := tok.SignedString([]byte(j.secret_key))

	if err != nil {
		return "", err
	}

	return token, err
}

func (j *Jwt) GenerateRefreshToken(sub string) (string, error) {
	claims := jwt.MapClaims{
		"sub": sub,
		"exp": time.Now().AddDate(0, 2, 0).Unix(),
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	token, err := tok.SignedString([]byte(j.secret_key))

	if err != nil {
		return "", err
	}

	return token, err
}

func (j *Jwt) Decode(token string) (map[string]any, error) {

	claims := jwt.MapClaims{}

	parsed, err := jwt.ParseWithClaims(
		token,
		claims,
		func(t *jwt.Token) (any, error) { return []byte(j.secret_key), nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
	)

	if err != nil {
		return nil, err
	}

	if !parsed.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	return claims, nil
}
