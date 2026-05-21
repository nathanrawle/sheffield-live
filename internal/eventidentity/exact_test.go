package eventidentity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func TestNormalizeCleanTitle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "collapses whitespace", input: "  The   Exact \n Title \t", want: "the exact title"},
		{name: "trims empty", input: " \n\t ", want: ""},
		{name: "lowercases", input: "MiXeD Case", want: "mixed case"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := NormalizeCleanTitle(tt.input); got != tt.want {
				t.Fatalf("NormalizeCleanTitle(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestMaterialTextAndBuildKey(t *testing.T) {
	t.Parallel()

	utc := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	bst := time.Date(2026, time.May, 12, 20, 0, 0, 0, time.FixedZone("BST", 3600))

	gotMaterial := MaterialText(ExactKeyVersion, " leadmill ", bst, "  The   Exact   Title  ")
	wantMaterial := "8:exact:v1\x008:leadmill\x0020:2026-05-12T19:00:00Z\x0015:the exact title\x00"
	if gotMaterial != wantMaterial {
		t.Fatalf("MaterialText(...) = %q, want %q", gotMaterial, wantMaterial)
	}

	sum := sha256.Sum256([]byte(wantMaterial))
	wantKey := fmt.Sprintf("exact:v1:%s", hex.EncodeToString(sum[:]))
	if gotKey := BuildKey(ExactKeyVersion, " leadmill ", bst, "  The   Exact   Title  "); gotKey != wantKey {
		t.Fatalf("BuildKey(...) = %q, want %q", gotKey, wantKey)
	}

	if gotUTC, gotBST := BuildKey(ExactKeyVersion, "leadmill", utc, "the exact title"), BuildKey(ExactKeyVersion, "leadmill", bst, "the exact title"); gotUTC != gotBST {
		t.Fatalf("BuildKey changed across equivalent UTC inputs: %q vs %q", gotUTC, gotBST)
	}
}

func TestBuildKeyChangesWhenIdentityInputsChange(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.May, 12, 19, 0, 0, 0, time.UTC)
	base := BuildKey(ExactKeyVersion, "leadmill", start, "shared title")

	if got := BuildKey(ExactKeyVersion, "other-venue", start, "shared title"); got == base {
		t.Fatalf("BuildKey did not change when venue changed: %q", got)
	}
	if got := BuildKey(ExactKeyVersion, "leadmill", start.Add(time.Hour), "shared title"); got == base {
		t.Fatalf("BuildKey did not change when start changed: %q", got)
	}
	if got := BuildKey(ExactKeyVersion, "leadmill", start, "different title"); got == base {
		t.Fatalf("BuildKey did not change when title changed: %q", got)
	}
}
