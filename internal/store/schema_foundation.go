package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type RepairRun struct {
	ID         int64
	StartedAt  time.Time
	FinishedAt time.Time
	Status     string
	Notes      string
}

type SlugAliasTargetKind string

const (
	SlugAliasTargetKindEvent SlugAliasTargetKind = "event"
	SlugAliasTargetKindVenue SlugAliasTargetKind = "venue"
)

type SlugAlias struct {
	ID            int64
	AliasSlug     string
	TargetKind    SlugAliasTargetKind
	TargetEventID *int64
	TargetVenueID *int64
	RepairRunID   *int64
	Reason        string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type EventExactIdentity struct {
	ID                int64
	EventID           int64
	IdentityKey       string
	KeyVersion        int
	VenueSlug         string
	UTCStartAt        time.Time
	CleanTitle        string
	Active            bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
	DeactivatedAt     time.Time
	DeactivatedReason string
	RepairRunID       *int64
}

type ObservationRunScope string

type ObservationRunScopeKind string

const (
	ObservationRunScopeKindImport ObservationRunScopeKind = "import"
	ObservationRunScopeKindRepair ObservationRunScopeKind = "repair"
)

func (s ObservationRunScope) String() string {
	return string(s)
}

func NewObservationRunScopeImport(importRunID int64) (ObservationRunScope, error) {
	return newObservationRunScope("import", importRunID)
}

func NewObservationRunScopeRepair(repairRunID int64) (ObservationRunScope, error) {
	return newObservationRunScope("repair", repairRunID)
}

func newObservationRunScope(prefix string, id int64) (ObservationRunScope, error) {
	if id <= 0 {
		return "", fmt.Errorf("%s run ID must be positive", prefix)
	}
	return ObservationRunScope(prefix + ":" + strconv.FormatInt(id, 10)), nil
}

func ParseObservationRunScope(scope ObservationRunScope) (ObservationRunScopeKind, int64, error) {
	value := string(scope)
	if strings.TrimSpace(value) != value {
		return "", 0, fmt.Errorf("observation run scope %q must not contain leading or trailing spaces", value)
	}

	prefix, suffix, ok := strings.Cut(value, ":")
	if !ok || prefix == "" || suffix == "" {
		return "", 0, fmt.Errorf("observation run scope %q must be import:<id> or repair:<id>", value)
	}
	switch ObservationRunScopeKind(prefix) {
	case ObservationRunScopeKindImport, ObservationRunScopeKindRepair:
	default:
		return "", 0, fmt.Errorf("observation run scope %q must start with import or repair", value)
	}
	if strings.TrimSpace(suffix) != suffix {
		return "", 0, fmt.Errorf("observation run scope %q must use a compact numeric identifier", value)
	}
	id, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != suffix {
		return "", 0, fmt.Errorf("observation run scope %q must use a positive integer identifier", value)
	}
	return ObservationRunScopeKind(prefix), id, nil
}

type SourceAuthority string

const (
	SourceAuthorityAuthoritative SourceAuthority = "authoritative"
	SourceAuthoritySupporting    SourceAuthority = "supporting"
)

type ObservationTargetKind string

const (
	ObservationTargetKindEvent              ObservationTargetKind = "event"
	ObservationTargetKindReviewGroup        ObservationTargetKind = "review_group"
	ObservationTargetKindEventReviewCluster ObservationTargetKind = "event_review_cluster"
)

type ObservationOutcome string

const (
	ObservationOutcomeApplied          ObservationOutcome = "applied"
	ObservationOutcomeFilledBlank      ObservationOutcome = "filled_blank"
	ObservationOutcomeConflictObserved ObservationOutcome = "conflict_observed"
	ObservationOutcomeStagedConflict   ObservationOutcome = "staged_conflict"
	ObservationOutcomeSkippedAmbiguous ObservationOutcome = "skipped_ambiguous"
	ObservationOutcomeStagedForReview  ObservationOutcome = "staged_for_review"
)

type SourceAttributeObservationInput struct {
	RunScope                  ObservationRunScope
	SourceID                  int64
	SourceIdentityKey         string
	SourceAuthority           SourceAuthority
	TargetKind                ObservationTargetKind
	EventID                   *int64
	ReviewGroupID             *int64
	EventReviewClusterID      *int64
	FieldName                 string
	IncomingRaw               string
	IncomingNormalized        string
	CanonicalBeforeRaw        string
	CanonicalBeforeNormalized string
	Outcome                   string
	IsConflict                bool
}
