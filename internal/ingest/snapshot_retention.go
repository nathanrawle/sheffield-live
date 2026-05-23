package ingest

import (
	"strings"
	"time"
)

type ImportRunSnapshotRetention struct {
	ImportRunID         int64
	LatestStartAt       *time.Time
	CandidateCount      int
	ParseableStartCount int
}

func SnapshotRetentionForReport(report Report) ImportRunSnapshotRetention {
	retention := ImportRunSnapshotRetention{ImportRunID: report.ImportRunID}
	for _, calendar := range report.Calendars {
		for _, candidate := range calendar.Candidates {
			retention.CandidateCount++
			startText := strings.TrimSpace(candidate.StartAt)
			if startText == "" {
				continue
			}
			startAt, err := time.Parse(time.RFC3339, startText)
			if err != nil {
				continue
			}
			startAt = startAt.UTC()
			retention.ParseableStartCount++
			if retention.LatestStartAt == nil || startAt.After(*retention.LatestStartAt) {
				value := startAt
				retention.LatestStartAt = &value
			}
		}
	}
	return retention
}
