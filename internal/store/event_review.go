package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const EventReviewExpectedVersion = 1

const EventReviewEvidenceRevisionAlgorithmVersion = 1

type EventReviewClusterStatus string

const (
	EventReviewClusterStatusOpen       EventReviewClusterStatus = "open"
	EventReviewClusterStatusResolved   EventReviewClusterStatus = "resolved"
	EventReviewClusterStatusDiscarded  EventReviewClusterStatus = "discarded"
	EventReviewClusterStatusSuperseded EventReviewClusterStatus = "superseded"
)

func (s EventReviewClusterStatus) Valid() bool {
	switch s {
	case EventReviewClusterStatusOpen, EventReviewClusterStatusResolved, EventReviewClusterStatusDiscarded, EventReviewClusterStatusSuperseded:
		return true
	default:
		return false
	}
}

type EventReviewCluster struct {
	ID                    int64
	Status                EventReviewClusterStatus
	Version               int
	StagingKey            *string
	StagingKeyVersion     int
	SupersededByClusterID *int64
	PreviousClusterID     *int64
	CanonicalEventID      *int64
	ConflictType          string
	ConflictReason        string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type EventReviewEvidence struct {
	ID                  int64
	SourceID            int64
	EventID             *int64
	EvidenceFingerprint string
	FingerprintVersion  int
	Payload             string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type EventReviewClusterEvidence struct {
	ID         int64
	ClusterID  int64
	EvidenceID int64
	Active     bool
	LinkedAt   time.Time
	UnlinkedAt *time.Time
	LinkReason string
}

type EventReviewIdentityKeyKind string

const (
	EventReviewIdentityKeyKindSource EventReviewIdentityKeyKind = "source"
	EventReviewIdentityKeyKindExact  EventReviewIdentityKeyKind = "exact"
	EventReviewIdentityKeyKindNear   EventReviewIdentityKeyKind = "near"
	EventReviewIdentityKeyKindManual EventReviewIdentityKeyKind = "manual"
)

func (k EventReviewIdentityKeyKind) Valid() bool {
	switch k {
	case EventReviewIdentityKeyKindSource, EventReviewIdentityKeyKindExact, EventReviewIdentityKeyKindNear, EventReviewIdentityKeyKindManual:
		return true
	default:
		return false
	}
}

type EventReviewIdentityKey struct {
	ID              int64
	IdentityKeyHash string
	KeyKind         EventReviewIdentityKeyKind
	KeyVersion      int
	NormalizedKey   string
	CreatedAt       time.Time
}

type EventReviewEvidenceIdentityKeyRole string

const (
	EventReviewEvidenceIdentityKeyRoleObserved EventReviewEvidenceIdentityKeyRole = "observed"
	EventReviewEvidenceIdentityKeyRoleDerived  EventReviewEvidenceIdentityKeyRole = "derived"
	EventReviewEvidenceIdentityKeyRoleExact    EventReviewEvidenceIdentityKeyRole = "exact"
)

func (r EventReviewEvidenceIdentityKeyRole) Valid() bool {
	switch r {
	case EventReviewEvidenceIdentityKeyRoleObserved, EventReviewEvidenceIdentityKeyRoleDerived, EventReviewEvidenceIdentityKeyRoleExact:
		return true
	default:
		return false
	}
}

type EventReviewEvidenceIdentityKey struct {
	ID            int64
	EvidenceID    int64
	IdentityKeyID int64
	SourceID      *int64
	Role          EventReviewEvidenceIdentityKeyRole
}

type EventReviewClusterIdentityKey struct {
	ID            int64
	ClusterID     int64
	IdentityKeyID int64
	Active        bool
	LinkedAt      time.Time
	UnlinkedAt    *time.Time
}

type EventReviewChoiceKind string

const (
	EventReviewChoiceKindEvent    EventReviewChoiceKind = "event"
	EventReviewChoiceKindEvidence EventReviewChoiceKind = "evidence"
	EventReviewChoiceKindManual   EventReviewChoiceKind = "manual"
)

func (k EventReviewChoiceKind) Valid() bool {
	switch k {
	case EventReviewChoiceKindEvent, EventReviewChoiceKindEvidence, EventReviewChoiceKindManual:
		return true
	default:
		return false
	}
}

type EventReviewRunKind string

const (
	EventReviewRunKindImport EventReviewRunKind = "import"
	EventReviewRunKindRepair EventReviewRunKind = "repair"
)

const (
	EventReviewConflictTypeImportReview      = "import_review"
	EventReviewConflictReasonIngestCandidate = "ingest_candidate"
)

func (k EventReviewRunKind) Valid() bool {
	switch k {
	case EventReviewRunKindImport, EventReviewRunKindRepair:
		return true
	default:
		return false
	}
}

type EventReviewRunRef struct {
	Kind EventReviewRunKind
	ID   int64
}

func (r EventReviewRunRef) Valid() bool {
	return r.Kind.Valid() && r.ID > 0
}

type StageEventReviewEvidenceInput struct {
	RunRef              EventReviewRunRef
	SourceID            int64
	SourceName          string
	SourceURL           string
	SourceAuthority     SourceAuthority
	StagingKey          string
	StagingKeyVersion   int
	ConflictType        string
	ConflictReason      string
	EventID             *int64
	EvidenceFingerprint string
	Payload             string
	SourceIdentityKeys  []string
	ExactIdentityKeys   []string
	WeakEvidence        bool
	WeakEvidenceReason  string
}

type StageEventReviewEvidenceResult struct {
	EvidenceID           int64
	ClusterID            int64
	ClusterStatus        EventReviewClusterStatus
	Version              int
	Created              bool
	Reused               bool
	Attached             bool
	ClusterCreated       bool
	ClusterReused        bool
	MergedClusterIDs     []int64
	SupersededClusterIDs []int64
	SkippedClusterIDs    []int64
	PreviousClusterID    *int64
	CanonicalEventID     *int64
	ConflictType         string
	ConflictReason       string
	AutoResolved         bool
	AutoResolvedResult   string
	CanonicalEventSlug   string
	RetryableConflict    bool
}

type StageRepairEventReviewEvidenceInput struct {
	RunRef              EventReviewRunRef
	SourceID            int64
	SourceName          string
	SourceURL           string
	SourceAuthority     SourceAuthority
	EventID             *int64
	EvidenceFingerprint string
	Payload             string
	SourceIdentityKeys  []string
	ExactIdentityKeys   []string
	WeakEvidence        bool
	WeakEvidenceReason  string
}

type EventReviewChoiceInput struct {
	FieldName  string
	ChoiceKind EventReviewChoiceKind
	EventID    *int64
	EvidenceID *int64
	Value      string
}

type EventReviewSourceIdentityChoiceInput struct {
	SourceID          int64
	SourceIdentityKey string
	Selected          bool
	SelectionReason   string
}

type SetEventReviewSourceIdentityChoicesInput struct {
	ClusterID       int64
	ExpectedVersion int
	Choices         []EventReviewSourceIdentityChoiceInput
}

type EventReviewLiveActionInput struct {
	EventID int64
	Action  EventReviewLiveActionKind
	Reason  string
}

type StageRepairEventReviewClusterInput struct {
	RunRef            EventReviewRunRef
	StagingKey        string
	StagingKeyVersion int
	ConflictType      string
	ConflictReason    string
	CanonicalEventID  *int64
	Evidence          []StageRepairEventReviewEvidenceInput
	CanonicalChoices  []EventReviewChoiceInput
	DraftChoices      []EventReviewChoiceInput
	LiveActions       []EventReviewLiveActionInput
}

type StageRepairEventReviewClusterResult struct {
	ClusterID      int64
	Version        int
	Status         EventReviewClusterStatus
	Created        bool
	Reused         bool
	TerminalReused bool
	EvidenceIDs    []int64
}

type EventReviewClusterSummary struct {
	ID                 int64
	Status             EventReviewClusterStatus
	Version            int
	StagingKey         *string
	StagingKeyVersion  int
	ConflictType       string
	ConflictReason     string
	CanonicalEventID   *int64
	CanonicalEventSlug string
	DisplayTitle       string
	DisplayVenueSlug   string
	DisplayVenueName   string
	DisplayStartAt     *time.Time
	EvidenceCount      int
	UpdatedAt          time.Time
	LatestImportRunID  *int64
	LatestRepairRunID  *int64
}

func (s EventReviewClusterSummary) ReviewKindLabel() string {
	if s.ConflictType == EventReviewConflictTypeImportReview && s.ConflictReason == EventReviewConflictReasonIngestCandidate {
		if s.EvidenceCount > 1 {
			return "Import candidate comparison"
		}
		return "Import listing candidate"
	}
	switch s.ConflictType {
	case "title_repair":
		return "Title repair"
	case "historical_duplicate":
		return "Historical duplicate"
	}
	if label := humanizeEventReviewQueueText(s.ConflictType); label != "" {
		return label
	}
	return "Event review"
}

func (s EventReviewClusterSummary) ReviewKindHint() string {
	if s.ConflictType == EventReviewConflictTypeImportReview && s.ConflictReason == EventReviewConflictReasonIngestCandidate {
		if s.EvidenceCount > 1 {
			return "Compare normalized candidates"
		}
		return "Review as a possible new event"
	}
	switch s.ConflictType {
	case "title_repair":
		return "Review clean title evidence"
	case "historical_duplicate":
		return "Review stored duplicate actions"
	}
	if hint := humanizeEventReviewQueueText(s.ConflictReason); hint != "" {
		return hint
	}
	return "Inspect cluster details"
}

func humanizeEventReviewQueueText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	if len(parts) == 0 {
		return ""
	}
	for i, part := range parts {
		if part == "" {
			continue
		}
		if len(part) == 1 {
			parts[i] = strings.ToUpper(part)
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
	}
	return strings.Join(parts, " ")
}

type EventReviewClusterHistorySummary struct {
	ID                    int64
	Status                EventReviewClusterStatus
	Version               int
	StagingKey            *string
	StagingKeyVersion     int
	ConflictType          string
	ConflictReason        string
	CanonicalEventID      *int64
	CanonicalEventSlug    string
	DisplayTitle          string
	DisplayVenueSlug      string
	DisplayVenueName      string
	DisplayStartAt        *time.Time
	EvidenceCount         int
	UpdatedAt             time.Time
	LatestImportRunID     *int64
	LatestRepairRunID     *int64
	ResolutionID          int64
	ResolutionStatus      EventReviewResolutionStatus
	DiscardReason         string
	ResolutionCreatedAt   time.Time
	ResolvedAt            time.Time
	SupersededByClusterID *int64
}

type EventReviewClusterEvidenceSummary struct {
	ID                  int64
	EvidenceID          int64
	SourceID            int64
	SourceName          string
	SourceURL           string
	EventID             *int64
	EventSlug           string
	EvidenceFingerprint string
	Payload             string
	LinkedAt            time.Time
	LinkReason          string
}

type EventReviewImportCandidateSummary struct {
	EvidenceID          int64
	EvidenceFingerprint string
	SourceID            int64
	SourceName          string
	SourceURL           string
	SourceAuthority     SourceAuthority
	EventID             *int64
	EventSlug           string
	ExternalID          string
	Title               string
	VenueSlug           string
	VenueText           string
	StartAt             *time.Time
	EndAt               *time.Time
	CalendarURL         string
}

type EventReviewImportReadiness struct {
	CandidateCount             int
	NewListingScope            bool
	CandidateComparisonScope   bool
	BlockingReasons            []string
	ComparisonBlockingReasons  []string
	PayloadWarnings            []string
	Candidates                 []EventReviewImportCandidateSummary
	ExistingEventTargets       []EventReviewImportExistingEventTarget
	AuthoritativeTargets       []EventReviewImportAuthoritativeTarget
	CandidateIdentityStatuses  []EventReviewImportCandidateIdentityStatus
	SelectedCandidateReadiness *EventReviewImportSelectedCandidateReadiness
	IdentityRows               []EventReviewImportIdentityRow
	RawRows                    []EventReviewImportComparisonRow
}

type EventReviewImportSelectedCandidateReadiness struct {
	Eligible             bool
	BlockingReasons      []string
	EvidenceID           int64
	EvidenceFingerprint  string
	EventID              *int64
	EventSlug            string
	Title                string
	VenueSlug            string
	VenueText            string
	StartAt              *time.Time
	SelectedSourceKeys   []EventReviewImportCandidateSourceIdentityStatus
	ExactKeys            []EventReviewImportCandidateExactIdentityStatus
	SourceKeys           []EventReviewImportCandidateSourceIdentityStatus
	ExistingEventTargets []EventReviewImportExistingEventTarget
}

type EventReviewImportTargetBasis string

const (
	EventReviewImportTargetBasisCanonicalEvent       EventReviewImportTargetBasis = "canonical_event"
	EventReviewImportTargetBasisEvidenceEvent        EventReviewImportTargetBasis = "evidence_event"
	EventReviewImportTargetBasisSourceIdentity       EventReviewImportTargetBasis = "source_identity"
	EventReviewImportTargetBasisExactIdentity        EventReviewImportTargetBasis = "exact_identity"
	EventReviewImportTargetBasisSlug                 EventReviewImportTargetBasis = "slug"
	EventReviewImportTargetBasisExactTitleVenueStart EventReviewImportTargetBasis = "exact_title_venue_start"
	EventReviewImportTargetBasisNearTitle            EventReviewImportTargetBasis = "near_title"
)

func (b EventReviewImportTargetBasis) Valid() bool {
	switch b {
	case EventReviewImportTargetBasisCanonicalEvent,
		EventReviewImportTargetBasisEvidenceEvent,
		EventReviewImportTargetBasisSourceIdentity,
		EventReviewImportTargetBasisExactIdentity,
		EventReviewImportTargetBasisSlug,
		EventReviewImportTargetBasisExactTitleVenueStart,
		EventReviewImportTargetBasisNearTitle:
		return true
	default:
		return false
	}
}

type EventReviewImportExistingEventTarget struct {
	EvidenceID                int64
	EvidenceFingerprint       string
	EventID                   int64
	EventSlug                 string
	EventTitle                string
	PublicationState          string
	TargetBasis               EventReviewImportTargetBasis
	SourceIdentityKeys        []string
	ExactIdentityKeys         []string
	ResolvedFromWithheld      bool
	RawLinkedEventID          *int64
	RawLinkedEventSlug        string
	RawLinkedPublicationState string
	BlockingReasons           []string
}

type EventReviewImportAuthoritativeTarget struct {
	EvidenceID          int64
	EvidenceFingerprint string
	Result              string
	EventID             *int64
	EventSlug           string
	EventTitle          string
	SourceIdentityKeys  []string
	BlockingReasons     []string
}

type EventReviewImportCandidateExactIdentityStatus struct {
	NormalizedKey    string
	IdentityKeyHash  string
	LinkedEventID    *int64
	LinkedEventSlug  string
	LinkedEventTitle string
}

type EventReviewImportCandidateSourceIdentityStatus struct {
	SourceID                  int64
	SourceName                string
	SourceIdentityKey         string
	LinkedEventID             *int64
	LinkedEventSlug           string
	LinkedEventTitle          string
	RawLinkedEventID          *int64
	RawLinkedEventSlug        string
	RawLinkedPublicationState string
	ResolvedFromWithheld      bool
	Authoritative             bool
	ChoiceSelected            bool
	ChoiceReason              string
	ChoiceUpdatedAt           *time.Time
}

type EventReviewImportCandidateIdentityStatus struct {
	EvidenceID          int64
	EvidenceFingerprint string
	SourceID            int64
	SourceName          string
	Title               string
	VenueSlug           string
	StartAt             *time.Time
	ParseWarning        string
	ExactKeys           []EventReviewImportCandidateExactIdentityStatus
	SourceKeys          []EventReviewImportCandidateSourceIdentityStatus
}

type EventReviewImportIdentityValue struct {
	EvidenceID int64
	Normalized string
	Raw        string
	Warning    string
}

type EventReviewImportIdentityRow struct {
	FieldName string
	Label     string
	Consensus bool
	Values    []EventReviewImportIdentityValue
}

type EventReviewImportComparisonValue struct {
	EvidenceID int64
	Value      string
}

type EventReviewImportComparisonRow struct {
	FieldName string
	Label     string
	Consensus bool
	Values    []EventReviewImportComparisonValue
}

type EventReviewClusterChoiceSummary struct {
	ID                  int64
	FieldName           string
	ChoiceKind          EventReviewChoiceKind
	EventID             *int64
	EventSlug           string
	EvidenceID          *int64
	EvidenceFingerprint string
	Value               string
	UpdatedAt           time.Time
}

type EventReviewClusterLiveActionSummary struct {
	ID        int64
	EventID   int64
	EventSlug string
	Action    EventReviewLiveActionKind
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type EventReviewResolutionAppliedLiveActionSummary struct {
	EventID   int64
	EventSlug string
	Action    EventReviewLiveActionKind
	Reason    string
}

type EventReviewResolutionAppliedTitleRepairSummary struct {
	EventID  int64
	OldTitle string
	NewTitle string
	OldSlug  string
	NewSlug  string
}

type EventReviewResolutionAppliedTitleSlugConflictSummary struct {
	Mode                EventReviewTitleRepairSlugConflictMode
	OldCanonicalEventID int64
	SlugConflictEventID int64
	SurvivingEventID    int64
	OldTitle            string
	NewTitle            string
	OldSlug             string
	NewSlug             string
}

type EventReviewResolutionAppliedAutoResolutionSummary struct {
	EventID       int64
	EventSlug     string
	Result        string
	SourceID      int64
	SourceName    string
	SourceURL     string
	EvidenceCount int
}

type EventReviewResolutionAppliedImportListingSummary struct {
	EventID    int64
	EventSlug  string
	Title      string
	VenueSlug  string
	VenueName  string
	StartAt    time.Time
	SourceID   int64
	SourceName string
	SourceURL  string
	EvidenceID int64
}

type EventReviewResolutionAppliedSupportingSourceSummary struct {
	EventID        int64
	EventSlug      string
	Title          string
	SourceID       int64
	SourceName     string
	SourceURL      string
	EvidenceID     int64
	TargetBasis    EventReviewImportTargetBasis
	PromotedReview bool
}

type EventReviewResolutionAppliedSeparationSummary struct {
	SeparationID int64
	EndpointAKey string
	EndpointBKey string
	Reason       string
}

type EventReviewResolutionAppliedAuthoritativeImportSummary struct {
	EventID    int64
	EventSlug  string
	Title      string
	SourceID   int64
	SourceName string
	SourceURL  string
	EvidenceID int64
	Result     string
}

type EventReviewClusterIdentityKeySummary struct {
	ID              int64
	IdentityKeyID   int64
	IdentityKeyHash string
	KeyKind         EventReviewIdentityKeyKind
	KeyVersion      int
	NormalizedKey   string
	LinkedAt        time.Time
}

type EventReviewEvidenceIdentityKeySummary struct {
	ID                  int64
	EvidenceID          int64
	EvidenceFingerprint string
	IdentityKeyID       int64
	IdentityKeyHash     string
	KeyKind             EventReviewIdentityKeyKind
	KeyVersion          int
	NormalizedKey       string
	SourceID            *int64
	Role                EventReviewEvidenceIdentityKeyRole
}

type EventReviewClusterSourceIdentityLinkSummary struct {
	SourceID                  int64
	SourceName                string
	SourceURL                 string
	SourceIdentityKey         string
	EvidenceCount             int
	LinkedEventID             *int64
	LinkedEventSlug           string
	LinkedEventTitle          string
	RawLinkedEventID          *int64
	RawLinkedEventSlug        string
	RawLinkedPublicationState string
	ResolvedFromWithheld      bool
	Authoritative             bool
	LinkUpdatedAt             *time.Time
}

type EventReviewClusterExactIdentityMatchSummary struct {
	IdentityKeyID        int64
	IdentityKeyHash      string
	KeyVersion           int
	NormalizedKey        string
	EvidenceCount        int
	LinkedEventID        *int64
	LinkedEventSlug      string
	LinkedEventTitle     string
	LinkedEventVenueSlug string
	LinkedEventStartAt   *time.Time
}

type EventReviewClusterObservationSummary struct {
	ID                        int64
	RunScope                  string
	SourceID                  int64
	SourceName                string
	SourceURL                 string
	SourceIdentityKey         string
	SourceAuthority           SourceAuthority
	FieldName                 string
	IncomingRaw               string
	IncomingNormalized        string
	CanonicalBeforeRaw        string
	CanonicalBeforeNormalized string
	Outcome                   string
	IsConflict                bool
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

type EventReviewSeparationEndpointSummary struct {
	Kind                EventReviewSeparationEndpointKind
	Key                 string
	EventID             *int64
	EventSlug           string
	EvidenceID          *int64
	EvidenceFingerprint string
	IdentityKeyID       *int64
	IdentityKeyHash     string
	IdentityKeyKind     EventReviewIdentityKeyKind
	NormalizedKey       string
	CanonicalEventID    *int64
	CanonicalEventSlug  string
}

type EventReviewClusterSeparationSummary struct {
	ID        int64
	EndpointA EventReviewSeparationEndpointSummary
	EndpointB EventReviewSeparationEndpointSummary
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type EventReviewResolutionSummary struct {
	ID                         int64
	ClusterID                  int64
	Status                     EventReviewResolutionStatus
	DiscardReason              string
	CanonicalEventID           *int64
	RepairRunID                *int64
	SupersededByClusterID      *int64
	AppliedAutoResolution      *EventReviewResolutionAppliedAutoResolutionSummary
	AppliedImportListing       *EventReviewResolutionAppliedImportListingSummary
	AppliedSupportingSource    *EventReviewResolutionAppliedSupportingSourceSummary
	AppliedAuthoritativeImport *EventReviewResolutionAppliedAuthoritativeImportSummary
	AppliedSeparations         []EventReviewResolutionAppliedSeparationSummary
	AppliedTitleRepair         *EventReviewResolutionAppliedTitleRepairSummary
	AppliedTitleSlugConflict   *EventReviewResolutionAppliedTitleSlugConflictSummary
	AppliedLiveActions         []EventReviewResolutionAppliedLiveActionSummary
	SnapshotRaw                string
	SnapshotParseWarning       string
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
}

type EventReviewClusterDetail struct {
	Summary              EventReviewClusterSummary
	Resolution           *EventReviewResolutionSummary
	ImportReadiness      *EventReviewImportReadiness
	TitleRepairReadiness *EventReviewTitleRepairReadiness
	Evidence             []EventReviewClusterEvidenceSummary
	ClusterIdentityKeys  []EventReviewClusterIdentityKeySummary
	EvidenceIdentityKeys []EventReviewEvidenceIdentityKeySummary
	SourceIdentityLinks  []EventReviewClusterSourceIdentityLinkSummary
	ExactIdentityMatches []EventReviewClusterExactIdentityMatchSummary
	Observations         []EventReviewClusterObservationSummary
	Separations          []EventReviewClusterSeparationSummary
	CanonicalChoices     []EventReviewClusterChoiceSummary
	DraftChoices         []EventReviewClusterChoiceSummary
	LiveActions          []EventReviewClusterLiveActionSummary
}

type EventReviewTitleRepairReadiness struct {
	CanonicalEventID                int64
	CurrentTitle                    string
	CurrentSlug                     string
	CurrentEventLive                bool
	DraftTitle                      string
	DraftSlug                       string
	Eligible                        bool
	BlockingReasons                 []string
	SlugConflictEventID             *int64
	SlugConflictEventSlug           string
	SlugConflictEventTitle          string
	SlugConflictResolutionAvailable bool
	SlugConflictBlockingReasons     []string
}

type EventReviewCanonicalChoice struct {
	ID         int64
	ClusterID  int64
	FieldName  string
	ChoiceKind EventReviewChoiceKind
	EventID    *int64
	EvidenceID *int64
	Value      string
	UpdatedAt  time.Time
}

type EventReviewDraftChoice struct {
	ID         int64
	ClusterID  int64
	FieldName  string
	ChoiceKind EventReviewChoiceKind
	EventID    *int64
	EvidenceID *int64
	Value      string
	UpdatedAt  time.Time
}

type EventReviewLiveActionKind string

const (
	EventReviewLiveActionKindWithholdDuplicate EventReviewLiveActionKind = "withhold_duplicate"
	EventReviewLiveActionKindKeepSeparate      EventReviewLiveActionKind = "keep_separate"
)

func (k EventReviewLiveActionKind) Valid() bool {
	switch k {
	case EventReviewLiveActionKindWithholdDuplicate, EventReviewLiveActionKindKeepSeparate:
		return true
	default:
		return false
	}
}

type EventReviewLiveAction struct {
	ID        int64
	ClusterID int64
	EventID   int64
	Action    EventReviewLiveActionKind
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type EventReviewSourceIdentityChoice struct {
	ID                int64
	ClusterID         int64
	SourceID          int64
	SourceIdentityKey string
	Selected          bool
	SelectionReason   string
	UpdatedAt         time.Time
}

type EventReviewSeparationEndpointKind string

const (
	EventReviewSeparationEndpointKindEvent       EventReviewSeparationEndpointKind = "event"
	EventReviewSeparationEndpointKindEvidence    EventReviewSeparationEndpointKind = "evidence"
	EventReviewSeparationEndpointKindIdentityKey EventReviewSeparationEndpointKind = "identity_key"
)

func (k EventReviewSeparationEndpointKind) Valid() bool {
	switch k {
	case EventReviewSeparationEndpointKindEvent, EventReviewSeparationEndpointKindEvidence, EventReviewSeparationEndpointKindIdentityKey:
		return true
	default:
		return false
	}
}

type EventReviewSeparationEndpoint struct {
	Kind             EventReviewSeparationEndpointKind
	Key              string
	EventID          *int64
	EvidenceID       *int64
	IdentityKeyID    *int64
	CanonicalEventID *int64
}

type EventReviewSeparation struct {
	ID        int64
	EndpointA EventReviewSeparationEndpoint
	EndpointB EventReviewSeparationEndpoint
	Active    bool
	Reason    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type EventReviewResolutionStatus string

const (
	EventReviewResolutionStatusResolved   EventReviewResolutionStatus = "resolved"
	EventReviewResolutionStatusDiscarded  EventReviewResolutionStatus = "discarded"
	EventReviewResolutionStatusSuperseded EventReviewResolutionStatus = "superseded"
)

func (s EventReviewResolutionStatus) Valid() bool {
	switch s {
	case EventReviewResolutionStatusResolved, EventReviewResolutionStatusDiscarded, EventReviewResolutionStatusSuperseded:
		return true
	default:
		return false
	}
}

type EventReviewResolution struct {
	ID            int64
	ClusterID     int64
	Status        EventReviewResolutionStatus
	Snapshot      string
	DiscardReason string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type EventReviewResolutionInput struct {
	ClusterID       int64
	ExpectedVersion int
}

type EventReviewAcceptSupportingSourceInput struct {
	EventReviewResolutionInput
	EvidenceID         int64
	TargetEventID      int64
	TargetBasis        EventReviewImportTargetBasis
	SourceIdentityKeys []string
}

type EventReviewImportSeparateAndInsertInput struct {
	EventReviewResolutionInput
	EvidenceID         int64
	NearTitleEventID   int64
	SourceIdentityKeys []string
}

type EventReviewImportAuthoritativeInput struct {
	EventReviewResolutionInput
	EvidenceID            int64
	ExpectedTargetEventID int64
	SourceIdentityKeys    []string
}

type EventReviewTitleRepairSlugConflictMode string

const (
	EventReviewTitleRepairSlugConflictModeMergeDuplicate       EventReviewTitleRepairSlugConflictMode = "merge_duplicate"
	EventReviewTitleRepairSlugConflictModeKeepSeparateNoChange EventReviewTitleRepairSlugConflictMode = "keep_separate_no_change"
)

func (m EventReviewTitleRepairSlugConflictMode) Valid() bool {
	switch m {
	case EventReviewTitleRepairSlugConflictModeMergeDuplicate, EventReviewTitleRepairSlugConflictModeKeepSeparateNoChange:
		return true
	default:
		return false
	}
}

type EventReviewTitleRepairSlugConflictInput struct {
	EventReviewResolutionInput
	Mode                     EventReviewTitleRepairSlugConflictMode
	OriginalCanonicalEventID int64
	SlugConflictEventID      int64
	DraftTitle               string
	DraftSlug                string
}

type EventReviewDiscardInput struct {
	EventReviewResolutionInput
	Reason string
}

type EventReviewSupersedeInput struct {
	EventReviewResolutionInput
	SupersededByClusterID int64
}

func NormalizeEventReviewSeparationEndpointValue(value string) string {
	return strings.TrimSpace(value)
}

func NormalizeEventReviewSeparationEndpointKey(kind EventReviewSeparationEndpointKind, value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", errors.New("event review separation endpoint value is required")
	}
	switch kind {
	case EventReviewSeparationEndpointKindEvent:
		return "event:" + trimmed, nil
	case EventReviewSeparationEndpointKindEvidence:
		return "evidence:" + trimmed, nil
	case EventReviewSeparationEndpointKindIdentityKey:
		return "identity:" + trimmed, nil
	default:
		return "", fmt.Errorf("unsupported event review separation endpoint kind %q", kind)
	}
}

func EventReviewSeparationEventEndpointKey(eventID int64) string {
	return "event:" + strconv.FormatInt(eventID, 10)
}
