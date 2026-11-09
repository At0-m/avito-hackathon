package narrative

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	recap "recap-personalization/internal/recap"
	"recap-personalization/internal/telemetry"
)

var (
	urlPattern    = regexp.MustCompile(`(?i)(https?://|www\.|\b[a-z0-9][a-z0-9.-]*\.(?:ru|com|net|org|io|ai)\b)`)
	numberPattern = regexp.MustCompile(`\d+(?:[.,]\d+)?`)
)

type output struct {
	SummaryTitle string `json:"summary_title"`
	SummaryText  string `json:"summary_text"`
}

type Service struct {
	Provider Provider
}

func (s Service) Build(ctx context.Context, input Input, topAchievementTitle string) recap.Narrative {
	fallback := fallbackNarrative(input, topAchievementTitle)
	if s.Provider == nil {
		// Template by configuration: no provider call, no timeout, no retry.
		telemetry.NarrativeOutcomes.Inc(telemetry.NarrativeProviderDisabled)
		return fallback
	}
	telemetry.NarrativeProviderCalls.Inc()
	result, err := s.Provider.Generate(ctx, input)
	if err != nil {
		log.Printf("mistral narrative fallback: provider error: %v", err)
		telemetry.NarrativeOutcomes.Inc(telemetry.NarrativeFallbackProviderErr)
		return fallback
	}
	parsed, err := parseAndValidate(result.Content, input.SafeFacts, input.Year)
	if err != nil {
		log.Printf("mistral narrative fallback: validation error: %v", err)
		telemetry.NarrativeOutcomes.Inc(telemetry.NarrativeFallbackValidation)
		return fallback
	}

	if strings.TrimSpace(result.Model) == "" {
		log.Printf("mistral narrative fallback: empty model")
		telemetry.NarrativeOutcomes.Inc(telemetry.NarrativeFallbackValidation)
		return fallback
	}
	telemetry.NarrativeOutcomes.Inc(telemetry.NarrativeProviderSuccess)
	model := result.Model
	return recap.Narrative{
		SummaryTitle:  parsed.SummaryTitle,
		SummaryText:   parsed.SummaryText,
		Source:        "mistral",
		Model:         &model,
		PromptVersion: recap.PromptVersion,
	}
}

func parseAndValidate(payload []byte, safeFacts []SafeFact, year int) (output, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var parsed output
	if err := decoder.Decode(&parsed); err != nil {
		return output{}, err
	}
	if err := ensureEOF(decoder); err != nil {
		return output{}, err
	}
	parsed.SummaryTitle = strings.TrimSpace(parsed.SummaryTitle)
	parsed.SummaryText = strings.TrimSpace(parsed.SummaryText)
	if parsed.SummaryTitle == "" || parsed.SummaryText == "" {
		return output{}, errors.New("empty narrative fields")
	}
	if utf8.RuneCountInString(parsed.SummaryTitle) > 120 || utf8.RuneCountInString(parsed.SummaryText) > 500 {
		return output{}, errors.New("narrative length exceeded")
	}
	combined := parsed.SummaryTitle + " " + parsed.SummaryText
	if urlPattern.MatchString(combined) {
		return output{}, errors.New("url is forbidden")
	}
	allowed := map[string]struct{}{strconv.Itoa(year): {}}
	for _, fact := range safeFacts {
		allowed[strconv.Itoa(fact.Value)] = struct{}{}
	}
	for _, number := range numberPattern.FindAllString(combined, -1) {
		if _, ok := allowed[number]; !ok {
			return output{}, errors.New("unsupported number")
		}
	}
	return parsed, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	}
	return errors.New("trailing json data")
}

func fallbackNarrative(input Input, topAchievementTitle string) recap.Narrative {
	text := "Главным районом стал «" + input.MainDistrict.Title + "». " +
		"Твоя роль - «" + input.Role.Title + "», стиль - «" + input.Style.Title + "»."
	if topAchievementTitle != "" {
		text += " Среди городских званий особенно выделяется «" + topAchievementTitle + "»."
	}
	return recap.Narrative{
		SummaryTitle:  "Твой город за год",
		SummaryText:   text,
		Source:        "template",
		PromptVersion: recap.PromptVersion,
	}
}
