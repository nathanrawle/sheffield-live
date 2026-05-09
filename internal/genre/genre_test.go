package genre

import "testing"

func TestInferRanksAcrossDescriptionsWithBalancedScore(t *testing.T) {
	rules := []Rule{
		{Key: "jazz", Name: "Jazz", MatchType: MatchTypePlain, Pattern: "jazz", Enabled: true, SortOrder: 10},
		{Key: "folk", Name: "Folk", MatchType: MatchTypePlain, Pattern: "folk", Enabled: true, SortOrder: 20},
		{Key: "hip-hop", Name: "Hip hop", MatchType: MatchTypeRegex, Pattern: `hip[- ]hop`, Enabled: true, SortOrder: 30},
	}
	matches, err := Infer([]string{
		"Folk opener before a jazz quartet.",
		"Late night hip-hop, jazz and more jazz.",
	}, rules)
	if err != nil {
		t.Fatalf("infer genres: %v", err)
	}
	if got, want := Summary(matches, 2), "Jazz, Folk"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if len(matches) != 3 {
		t.Fatalf("matches = %d, want 3", len(matches))
	}
	if matches[0].MentionCount != 3 {
		t.Fatalf("jazz mention count = %d, want 3", matches[0].MentionCount)
	}
}

func TestInferPlainMatchUsesBoundaries(t *testing.T) {
	rules := []Rule{
		{Key: "rap", Name: "Rap", MatchType: MatchTypePlain, Pattern: "rap", Enabled: true},
	}
	matches, err := Infer([]string{"Rapid songs, then rap."}, rules)
	if err != nil {
		t.Fatalf("infer genres: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("matches = %d, want 1", len(matches))
	}
	if matches[0].MentionCount != 1 {
		t.Fatalf("mention count = %d, want 1", matches[0].MentionCount)
	}
}

func TestInferMergesMultipleRulesForSameGenreName(t *testing.T) {
	rules := []Rule{
		{Key: "hip-hop", Name: "Hip hop", MatchType: MatchTypeRegex, Pattern: `hip[- ]hop`, Enabled: true},
		{Key: "rap", Name: "Hip hop", MatchType: MatchTypePlain, Pattern: "rap", Enabled: true},
	}
	matches, err := Infer([]string{"Hip-hop night with rap support."}, rules)
	if err != nil {
		t.Fatalf("infer genres: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("matches = %d, want 1", len(matches))
	}
	if matches[0].MentionCount != 2 {
		t.Fatalf("mention count = %d, want 2", matches[0].MentionCount)
	}
}

func TestValidateRuleRejectsInvalidRegex(t *testing.T) {
	err := ValidateRule(Rule{
		Key:       "broken",
		Name:      "Broken",
		MatchType: MatchTypeRegex,
		Pattern:   "[",
		Enabled:   true,
	})
	if err == nil {
		t.Fatal("expected invalid regex error")
	}
}
