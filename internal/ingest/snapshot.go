package ingest

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
)

type SnapshotEnvelope struct {
	Version   int                     `json:"version"`
	Metadata  SnapshotContentMetadata `json:"metadata"`
	SHA256    string                  `json:"sha256"`
	Truncated bool                    `json:"truncated"`
	Body      string                  `json:"body_base64"`
}

type SnapshotContentMetadata struct {
	URL           string `json:"url"`
	FinalURL      string `json:"final_url,omitempty"`
	Status        string `json:"status,omitempty"`
	StatusCode    int    `json:"status_code,omitempty"`
	ContentType   string `json:"content_type,omitempty"`
	ContentLength int64  `json:"content_length,omitempty"`
	BodyBytes     int    `json:"body_bytes"`
	CapturedAt    string `json:"captured_at"`
}

func NewSnapshotEnvelope(result FetchResult) SnapshotEnvelope {
	sum := sha256.Sum256(result.Body)
	return SnapshotEnvelope{
		Version: 1,
		Metadata: SnapshotContentMetadata{
			URL:           result.URL,
			FinalURL:      result.FinalURL,
			Status:        result.Status,
			StatusCode:    result.StatusCode,
			ContentType:   result.ContentType,
			ContentLength: result.ContentLength,
			BodyBytes:     len(result.Body),
			CapturedAt:    formatTime(result.CapturedAt),
		},
		SHA256:    hex.EncodeToString(sum[:]),
		Truncated: result.Truncated,
		Body:      base64.StdEncoding.EncodeToString(result.Body),
	}
}

func (e SnapshotEnvelope) JSON() (string, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
