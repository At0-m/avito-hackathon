package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"recap-personalization/internal/model"
	recap "recap-personalization/internal/recap"
	"recap-personalization/internal/recap/ports"
	"recap-personalization/internal/repository"

	"github.com/google/uuid"
)

func (s *Service) GenerateRecap(ctx context.Context, profileID string, year int) (*model.Recap, bool, error) {
	existing, err := s.recaps.GetRecapByProfileAndYear(ctx, profileID, year)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, repository.ErrRecapNotFound) {
		return nil, false, err
	}

	value, err := s.generateRecapValue(ctx, uuid.New().String(), profileID, year, nil)
	if err != nil {
		return nil, false, err
	}
	if err := s.recaps.CreateRecap(ctx, value); err != nil {
		if errors.Is(err, repository.ErrRecapAlreadyExists) {
			existing, readErr := s.recaps.GetRecapByProfileAndYear(ctx, profileID, year)
			if readErr != nil {
				return nil, false, readErr
			}
			return existing, false, nil
		}
		return nil, false, err
	}
	return value, true, nil
}

type progressReporter func(stage string, progress int) error

func (s *Service) generateRecapValue(
	ctx context.Context,
	recapID string,
	profileID string,
	year int,
	report progressReporter,
) (*model.Recap, error) {
	profile, err := s.profiles.GetProfileByID(ctx, profileID)
	if err != nil {
		return nil, err
	}
	if !containsYear(profile.AvailableYears, year) {
		return nil, ErrYearNotAvailable
	}
	if s.clickHouseActivities == nil {
		return nil, ErrActivitySourceMissing
	}
	if s.recapGenerator == nil {
		return nil, errors.New("recap_generator_missing")
	}

	activities, err := s.clickHouseActivities.GetActivitiesByProfileIDAndYear(ctx, profileID, year)
	if err != nil {
		log.Printf("failed to load ClickHouse activities: %v", err)
		return nil, fmt.Errorf("%w: %v", ErrActivitySourceUnavailable, err)
	}
	if report != nil {
		if err := report("computing_features", 35); err != nil {
			return nil, err
		}
	}
	recapEvents, err := convertActivities(activities)
	if err != nil {
		return nil, err
	}
	if report != nil {
		if err := report("personalizing", 55); err != nil {
			return nil, err
		}
		if err := report("generating_narrative", 75); err != nil {
			return nil, err
		}
	}

	output, err := s.recapGenerator.Generate(ctx, recap.GenerateInput{
		RecapID:     recapID,
		Profile:     convertProfile(profile),
		Year:        year,
		Activities:  recapEvents,
		GeneratedAt: time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	return convertRecapOutput(output)
}

func convertActivities(values []ports.ActivityEvent) ([]recap.ActivityEvent, error) {
	result := make([]recap.ActivityEvent, 0, len(values))
	for _, value := range values {
		eventType, err := convertEventType(value.EventType)
		if err != nil {
			return nil, err
		}
		result = append(result, recap.ActivityEvent{
			EventID:      value.EventID,
			ProfileID:    value.ProfileID,
			EventType:    eventType,
			VerticalCode: value.VerticalCode,
			CategoryCode: value.CategoryCode,
			OccurredAt:   value.OccurredAt.UTC(),
		})
	}
	return result, nil
}

func convertEventType(value string) (recap.EventType, error) {
	switch value {
	case string(recap.EventListingViewed):
		return recap.EventListingViewed, nil
	case string(recap.EventFavoriteAdded):
		return recap.EventFavoriteAdded, nil
	case string(recap.EventSearchSaved):
		return recap.EventSearchSaved, nil
	case string(recap.EventChatStarted):
		return recap.EventChatStarted, nil
	case string(recap.EventListingPublished):
		return recap.EventListingPublished, nil
	case string(recap.EventSaleCompleted):
		return recap.EventSaleCompleted, nil
	case string(recap.EventPurchaseCompleted):
		return recap.EventPurchaseCompleted, nil
	case string(recap.EventDeliveryUsed):
		return recap.EventDeliveryUsed, nil
	default:
		return "", fmt.Errorf("unsupported activity event type %q", value)
	}
}

func convertProfile(profile *model.Profile) recap.ProfileSnapshot {
	avatarURL := ""
	if profile.AvatarURL != nil {
		avatarURL = *profile.AvatarURL
	}
	return recap.ProfileSnapshot{
		ID:        profile.ID.String(),
		Name:      profile.Name,
		AvatarURL: avatarURL,
	}
}

func convertRecapOutput(output recap.GenerateOutput) (*model.Recap, error) {
	recapID, err := uuid.Parse(output.Recap.ID)
	if err != nil {
		return nil, fmt.Errorf("parse recap id: %w", err)
	}
	profileID, err := uuid.Parse(output.Recap.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("parse profile id: %w", err)
	}
	profileSnapshotID, err := uuid.Parse(output.Recap.Profile.ID)
	if err != nil {
		return nil, fmt.Errorf("parse recap profile id: %w", err)
	}

	value := &model.Recap{
		SchemaVersion: output.Recap.SchemaVersion,
		ID:            recapID,
		ProfileID:     profileID,
		Year:          output.Recap.Year,
		Profile: model.RecapProfile{
			ID:        profileSnapshotID,
			Name:      output.Recap.Profile.Name,
			AvatarURL: output.Recap.Profile.AvatarURL,
		},
		Generation: model.RecapGeneration{
			AlgorithmVersion:     output.Recap.Generation.AlgorithmVersion,
			FeatureSchemaVersion: output.Recap.Generation.FeatureSchemaVersion,
			ActivityHash:         output.Recap.Generation.ActivityHash,
			GeneratedAt:          output.Recap.Generation.GeneratedAt,
			Narrative: model.NarrativeGeneration{
				Source:        output.Recap.Generation.Narrative.Source,
				PromptVersion: output.Recap.Generation.Narrative.PromptVersion,
				Model:         output.Recap.Generation.Narrative.Model,
			},
		},
		Theme: model.RecapTheme{
			Code: output.Recap.Theme.Code,
			MainDistrict: model.Vertical{
				Code:  string(output.Recap.Theme.MainDistrict.Code),
				Title: output.Recap.Theme.MainDistrict.Title,
			},
			AccentToken: accentTokenPointer(output.Recap.Theme.AccentToken),
		},
		Cards:        convertCards(output.Recap.Cards),
		Capabilities: convertCapabilities(output.Recap.Capabilities),
		Metrics:      output.Metrics,
		Archetype:    output.Archetype,
		Achievements: output.Achievements,
		Narrative:    output.Narrative,
		Explanation:  output.Explanation,
		Share:        output.Share,
	}
	return value, nil
}

func convertCards(cards []recap.RecapCard) []model.RecapCard {
	result := make([]model.RecapCard, 0, len(cards))
	for _, card := range cards {
		base := card.Base()
		data := make(map[string]interface{})
		switch value := card.(type) {
		case recap.IntroCard:
			data["year"] = value.Data.Year
		case recap.MetricCard:
			data["metric_code"] = value.Data.MetricCode
			data["value"] = value.Data.Value
			if value.Data.Unit != "" {
				data["unit"] = value.Data.Unit
			}
			if value.Data.SecondaryLabel != nil {
				data["secondary_label"] = value.Data.SecondaryLabel
			}
		case recap.DistrictCard:
			data["vertical"] = value.Data.Vertical
			data["activity_share"] = value.Data.ActivityShare
			if value.Data.TopCategory != nil {
				data["top_category"] = value.Data.TopCategory
			}
		case recap.ArchetypeCard:
			data["role"] = value.Data.Role
			data["style"] = value.Data.Style
		case recap.AchievementsCard:
			data["items"] = value.Data.Items
			data["total_count"] = value.Data.TotalCount
		case recap.SummaryCard:
			data["role_code"] = value.Data.RoleCode
			data["style_code"] = value.Data.StyleCode
			data["achievement_codes"] = value.Data.AchievementCodes
		case recap.FinalCard:
			data["show_share_button"] = value.Data.ShowShareButton
			data["show_feedback"] = value.Data.ShowFeedback
		}

		result = append(result, model.RecapCard{
			ID:          base.ID,
			Type:        string(base.Type),
			Position:    base.Position,
			Visibility:  string(base.Visibility),
			Eyebrow:     base.Eyebrow,
			Title:       base.Title,
			Description: base.Description,
			Visual:      convertVisual(base.Visual),
			Explainable: base.Explainable,
			Data:        data,
		})
	}
	return result
}

func convertVisual(value *recap.CardVisual) model.CardVisual {
	if value == nil {
		return model.CardVisual{}
	}
	return model.CardVisual{
		Kind:      string(value.Kind),
		AssetCode: value.AssetCode,
	}
}

func convertCapabilities(value recap.Capabilities) model.RecapCapabilities {
	return model.RecapCapabilities{
		ShareAvailable:       value.ShareAvailable,
		ExplanationAvailable: value.ExplanationAvailable,
		FeedbackAvailable:    value.FeedbackAvailable,
	}
}

func accentTokenPointer(value *recap.AccentToken) *string {
	if value == nil {
		return nil
	}
	converted := string(*value)
	return &converted
}
