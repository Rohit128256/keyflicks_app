package signature

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
)

func md5Hex(data []byte) string {
	sum := md5.Sum(data)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func signURI(pathURI string, expiresTS int64, uri_secret string) string {
	raw := []byte(fmt.Sprintf("%d%s%s", expiresTS, pathURI, uri_secret))
	return md5Hex(raw)
}

var playlistLineRe = regexp.MustCompile(`(?m)^([^#\s].*)$`)

func RewritePlaylist(playlistContent string, videoID string, resolutionPath string, expires int64, uri_secret string) string {
	replacer := func(line string) string {
		segmentFile := line
		publicPath := fmt.Sprintf("/videos/%s/%s/%s", videoID, resolutionPath, segmentFile)
		sig := signURI(publicPath, expires, uri_secret)
		return fmt.Sprintf("%s?st=%d&sig=%s", publicPath, expires, sig)
	}
	return playlistLineRe.ReplaceAllStringFunc(playlistContent, replacer)
}

func RewriteMasterPlaylist(playlistContent string, videoID string) string {
	// 1. Split the playlist content into individual lines.
	// Equivalent to Python's `playlist_content.splitlines()`
	lines := strings.Split(playlistContent, "\n")

	// 2. Create a slice to hold the new, rewritten lines.
	rewrittenLines := make([]string, 0, len(lines))

	// 3. Loop through each line, just like the Python list comprehension.
	for _, line := range lines {
		// Equivalent to `if line.strip() == "" or line.startswith("#"):`
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "#") {
			// If the line is a comment or empty, add it unchanged.
			rewrittenLines = append(rewrittenLines, line)
			continue
		}

		// Equivalent to `parts = line.split('/', 1)`
		// We split into at most 2 parts to get the resolution directory.
		parts := strings.SplitN(trimmedLine, "/", 2) // trimmedLine is like eg: "360p/playlist.m3u8"
		if len(parts) == 0 {
			continue // Should not happen, but a safe check
		}
		resolutionDir := parts[0]

		// Equivalent to `f"/api/playlist/{video_id}/{resolution_dir.lstrip('/').rstrip('/')}"`
		// We use fmt.Sprintf for formatting.
		playlistPath := fmt.Sprintf("/api/playlist/%s/%s", videoID, resolutionDir)

		// Add the rewritten line to our slice.
		rewrittenLines = append(rewrittenLines, playlistPath)
	}

	// 4. Join the lines back together with newlines and add a final newline.
	// Equivalent to `"\n".join(rewritten_lines) + "\n"`
	return strings.Join(rewrittenLines, "\n") + "\n"
}
