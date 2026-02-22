package security

import (
	"fmt"
	"net"
	"net/mail"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

func HashPassword(password string) (string, error) {
	hashed_pass, err := bcrypt.GenerateFromPassword([]byte(password), 11)
	if err != nil {
		return "", fmt.Errorf("error : %w", err)
	}

	return string(hashed_pass), nil
}

func VerifyPassword(hashedPass, plainPass string) error {
	return bcrypt.CompareHashAndPassword([]byte(hashedPass), []byte(plainPass))
}

func IsEmailLikelyValid(email string) bool {
	if _, err := mail.ParseAddress(email); err != nil {
		return false
	}
	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return false
	}
	mx, err := net.LookupMX(parts[1])
	return err == nil && len(mx) > 0
}
