package genre

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	MatchTypePlain = "plain"
	MatchTypeRegex = "regex"
)

type Rule struct {
	ID            int64
	Key           string
	Name          string
	MatchType     string
	Pattern       string
	Enabled       bool
	SortOrder     int
	AdminModified bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type RuleInput struct {
	ID        int64
	Key       string
	Name      string
	MatchType string
	Pattern   string
	Enabled   bool
	SortOrder int
}

type Match struct {
	Name             string
	Rank             int
	Score            float64
	MentionCount     int
	EarliestPosition int
}

type defaultsFile struct {
	Rules []ruleYAML `yaml:"rules"`
}

type ruleYAML struct {
	Key       string `yaml:"key"`
	Name      string `yaml:"name"`
	MatchType string `yaml:"match_type"`
	Pattern   string `yaml:"pattern"`
	Enabled   *bool  `yaml:"enabled"`
	SortOrder int    `yaml:"sort_order"`
}

func LoadRepoDefaults() ([]Rule, error) {
	return LoadDefaults(defaultsPath())
}

func LoadDefaults(path string) ([]Rule, error) {
	raw, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, fmt.Errorf("read genre defaults: %w", err)
	}
	var parsed defaultsFile
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode genre defaults: %w", err)
	}
	if len(parsed.Rules) == 0 {
		return nil, errors.New("genre defaults must define at least one rule")
	}

	rules := make([]Rule, 0, len(parsed.Rules))
	seen := make(map[string]struct{}, len(parsed.Rules))
	for i, item := range parsed.Rules {
		enabled := true
		if item.Enabled != nil {
			enabled = *item.Enabled
		}
		rule := Rule{
			Key:       strings.TrimSpace(item.Key),
			Name:      strings.TrimSpace(item.Name),
			MatchType: strings.TrimSpace(item.MatchType),
			Pattern:   strings.TrimSpace(item.Pattern),
			Enabled:   enabled,
			SortOrder: item.SortOrder,
		}
		if rule.SortOrder == 0 {
			rule.SortOrder = (i + 1) * 10
		}
		if err := ValidateRule(rule); err != nil {
			return nil, fmt.Errorf("genre rule %q: %w", firstNonEmpty(rule.Key, rule.Name), err)
		}
		if _, ok := seen[rule.Key]; ok {
			return nil, fmt.Errorf("duplicate genre rule key %q", rule.Key)
		}
		seen[rule.Key] = struct{}{}
		rules = append(rules, rule)
	}
	return rules, nil
}

func defaultsPath() string {
	dir, err := os.Getwd()
	if err != nil {
		return filepath.Join("config", "genres.yaml")
	}
	for {
		candidate := filepath.Join(dir, "config", "genres.yaml")
		if info, statErr := os.Stat(candidate); statErr == nil && !info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return filepath.Join("config", "genres.yaml")
		}
		dir = parent
	}
}

func ValidateRule(rule Rule) error {
	if strings.TrimSpace(rule.Key) == "" {
		return errors.New("key is required")
	}
	if strings.TrimSpace(rule.Name) == "" {
		return errors.New("name is required")
	}
	if strings.TrimSpace(rule.Pattern) == "" {
		return errors.New("pattern is required")
	}
	switch strings.TrimSpace(rule.MatchType) {
	case MatchTypePlain:
		return nil
	case MatchTypeRegex:
		if _, err := regexp.Compile(caseInsensitivePattern(rule.Pattern)); err != nil {
			return fmt.Errorf("invalid regex pattern: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("match_type must be %q or %q", MatchTypePlain, MatchTypeRegex)
	}
}

func RuleFromInput(input RuleInput) Rule {
	return Rule{
		ID:        input.ID,
		Key:       firstNonEmpty(strings.TrimSpace(input.Key), KeyFromName(input.Name)),
		Name:      strings.TrimSpace(input.Name),
		MatchType: strings.TrimSpace(input.MatchType),
		Pattern:   strings.TrimSpace(input.Pattern),
		Enabled:   input.Enabled,
		SortOrder: input.SortOrder,
	}
}

func KeyFromName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	wroteDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			wroteDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			wroteDash = false
		default:
			if b.Len() > 0 && !wroteDash {
				b.WriteByte('-')
				wroteDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func Infer(descriptions []string, rules []Rule) ([]Match, error) {
	corpus := joinedDescriptions(descriptions)
	if corpus == "" {
		return nil, nil
	}
	enabled := make([]Rule, 0, len(rules))
	for _, rule := range rules {
		if rule.Enabled {
			enabled = append(enabled, rule)
		}
	}
	sort.SliceStable(enabled, func(i, j int) bool {
		if enabled[i].SortOrder == enabled[j].SortOrder {
			return enabled[i].Name < enabled[j].Name
		}
		return enabled[i].SortOrder < enabled[j].SortOrder
	})

	type aggregate struct {
		name             string
		mentionCount     int
		earliestPosition int
	}
	byName := make(map[string]aggregate)
	for _, rule := range enabled {
		if err := ValidateRule(rule); err != nil {
			return nil, err
		}
		positions, err := matchPositions(corpus, rule)
		if err != nil {
			return nil, err
		}
		if len(positions) == 0 {
			continue
		}
		earliest := positions[0]
		for _, pos := range positions[1:] {
			if pos < earliest {
				earliest = pos
			}
		}
		existing, ok := byName[rule.Name]
		if !ok {
			byName[rule.Name] = aggregate{name: rule.Name, mentionCount: len(positions), earliestPosition: earliest}
			continue
		}
		existing.mentionCount += len(positions)
		if earliest < existing.earliestPosition {
			existing.earliestPosition = earliest
		}
		byName[rule.Name] = existing
	}

	matches := make([]Match, 0, len(byName))
	for _, item := range byName {
		earliestBonus := 20 * (1 - (float64(item.earliestPosition) / float64(len(corpus))))
		matches = append(matches, Match{
			Name:             item.name,
			Score:            float64(item.mentionCount)*10 + earliestBonus,
			MentionCount:     item.mentionCount,
			EarliestPosition: item.earliestPosition,
		})
	}

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Score != matches[j].Score {
			return matches[i].Score > matches[j].Score
		}
		if matches[i].MentionCount != matches[j].MentionCount {
			return matches[i].MentionCount > matches[j].MentionCount
		}
		if matches[i].EarliestPosition != matches[j].EarliestPosition {
			return matches[i].EarliestPosition < matches[j].EarliestPosition
		}
		return matches[i].Name < matches[j].Name
	})
	for i := range matches {
		matches[i].Rank = i + 1
	}
	return matches, nil
}

func Summary(matches []Match, limit int) string {
	if limit <= 0 || len(matches) == 0 {
		return ""
	}
	if len(matches) < limit {
		limit = len(matches)
	}
	names := make([]string, 0, limit)
	for _, match := range matches[:limit] {
		names = append(names, match.Name)
	}
	return strings.Join(names, ", ")
}

func Names(matches []Match) []string {
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		names = append(names, match.Name)
	}
	return names
}

func joinedDescriptions(descriptions []string) string {
	out := make([]string, 0, len(descriptions))
	seen := make(map[string]struct{}, len(descriptions))
	for _, description := range descriptions {
		description = strings.TrimSpace(description)
		if description == "" {
			continue
		}
		if _, ok := seen[description]; ok {
			continue
		}
		seen[description] = struct{}{}
		out = append(out, description)
	}
	return strings.Join(out, "\n\n")
}

func matchPositions(corpus string, rule Rule) ([]int, error) {
	switch rule.MatchType {
	case MatchTypePlain:
		pattern := `(?i)(^|[^[:alnum:]])(` + regexp.QuoteMeta(rule.Pattern) + `)([^[:alnum:]]|$)`
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		indexes := re.FindAllStringSubmatchIndex(corpus, -1)
		positions := make([]int, 0, len(indexes))
		for _, idx := range indexes {
			if len(idx) >= 6 && idx[4] >= 0 {
				positions = append(positions, idx[4])
			}
		}
		return positions, nil
	case MatchTypeRegex:
		re, err := regexp.Compile(caseInsensitivePattern(rule.Pattern))
		if err != nil {
			return nil, err
		}
		indexes := re.FindAllStringIndex(corpus, -1)
		positions := make([]int, 0, len(indexes))
		for _, idx := range indexes {
			if len(idx) >= 2 && idx[0] >= 0 && idx[1] > idx[0] {
				positions = append(positions, idx[0])
			}
		}
		return positions, nil
	default:
		return nil, fmt.Errorf("unknown match type %q", rule.MatchType)
	}
}

func caseInsensitivePattern(pattern string) string {
	return "(?i)" + strings.TrimSpace(pattern)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
