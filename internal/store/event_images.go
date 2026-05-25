package store

type EventImage struct {
	ImageURL          string
	ImageSourceURL    string
	Alt               string
	Width             int
	Height            int
	FocusX            int
	FocusY            int
	SourceName        string
	ListingURL        string
	SourceIdentityKey string
	Canonical         bool
}
