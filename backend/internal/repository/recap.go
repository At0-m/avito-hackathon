package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"recap-personalization/internal/model"
	recap "recap-personalization/internal/recap"
)

var (
	ErrRecapNotFound      = errors.New("recap_not_found")
	ErrRecapAlreadyExists = errors.New("recap_already_exists")
)

func (r *Repository) GetRecapByProfileAndYear(ctx context.Context, profileID string, year int) (*model.Recap, error) {
	return r.getRecap(ctx, `
		SELECT
			id,
			profile_id,
			year,
			profile_name,
			profile_avatar_url,
			schema_version,
			algorithm_version,
			feature_schema_version,
			activity_hash,
			generated_at,
			narrative_source,
			prompt_version,
			narrative_model,
			main_vertical_code,
			main_vertical_title,
			accent_token,
			share_available,
			explanation_available,
			feedback_available,
			summary_title,
			summary_text
		FROM recaps
		WHERE profile_id = $1 AND year = $2
	`, profileID, year)
}

func (r *Repository) GetRecapByID(ctx context.Context, id string) (*model.Recap, error) {
	return r.getRecap(ctx, `
		SELECT
			id,
			profile_id,
			year,
			profile_name,
			profile_avatar_url,
			schema_version,
			algorithm_version,
			feature_schema_version,
			activity_hash,
			generated_at,
			narrative_source,
			prompt_version,
			narrative_model,
			main_vertical_code,
			main_vertical_title,
			accent_token,
			share_available,
			explanation_available,
			feedback_available,
			summary_title,
			summary_text
		FROM recaps
		WHERE id = $1
	`, id)
}

func (r *Repository) getRecap(ctx context.Context, query string, args ...interface{}) (*model.Recap, error) {
	var value model.Recap
	var avatarURL sql.NullString
	var narrativeModel sql.NullString
	var accentToken sql.NullString
	var summaryTitle string
	var summaryText string

	err := r.DB.DB.QueryRowContext(ctx, query, args...).Scan(
		&value.ID,
		&value.ProfileID,
		&value.Year,
		&value.Profile.Name,
		&avatarURL,
		&value.SchemaVersion,
		&value.Generation.AlgorithmVersion,
		&value.Generation.FeatureSchemaVersion,
		&value.Generation.ActivityHash,
		&value.Generation.GeneratedAt,
		&value.Generation.Narrative.Source,
		&value.Generation.Narrative.PromptVersion,
		&narrativeModel,
		&value.Theme.MainDistrict.Code,
		&value.Theme.MainDistrict.Title,
		&accentToken,
		&value.Capabilities.ShareAvailable,
		&value.Capabilities.ExplanationAvailable,
		&value.Capabilities.FeedbackAvailable,
		&summaryTitle,
		&summaryText,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRecapNotFound
	}
	if err != nil {
		return nil, err
	}

	value.Profile.ID = value.ProfileID
	value.Profile.AvatarURL = nullableStringPointer(avatarURL)
	value.Generation.Narrative.Model = nullableStringPointer(narrativeModel)
	value.Theme.Code = "city"
	value.Theme.AccentToken = nullableStringPointer(accentToken)
	value.Narrative = recap.Narrative{
		SummaryTitle:  summaryTitle,
		SummaryText:   summaryText,
		Source:        value.Generation.Narrative.Source,
		Model:         value.Generation.Narrative.Model,
		PromptVersion: value.Generation.Narrative.PromptVersion,
	}

	if err := r.loadCards(ctx, &value); err != nil {
		return nil, err
	}
	if value.Capabilities.ExplanationAvailable {
		explanation, err := r.loadExplanation(ctx, &value)
		if err != nil {
			return nil, err
		}
		value.Explanation = explanation
	}
	if value.Capabilities.ShareAvailable {
		share, err := r.loadShare(ctx, value.ID.String())
		if err != nil {
			return nil, err
		}
		value.Share = share
	}
	return &value, nil
}

func (r *Repository) CreateRecap(ctx context.Context, value *model.Recap) error {
	return r.DB.WithinTransaction(ctx, nil, func(tx *sql.Tx) error {
		return insertRecapSnapshotTx(ctx, tx, value)
	})
}

func (r *Repository) FinalizeRecapRequest(
	ctx context.Context,
	requestID, workerID string,
	value *model.Recap,
) error {
	return r.DB.WithinTransaction(ctx, nil, func(tx *sql.Tx) error {
		if err := insertRecapSnapshotTx(ctx, tx, value); err != nil {
			return err
		}
		return requireOwnedRequestUpdate(ctx, tx, `
			UPDATE recap_requests
			SET
				status = 'ready',
				stage = 'ready',
				progress_percent = 100,
				recap_id = $3,
				worker_id = NULL,
				locked_at = NULL,
				lease_expires_at = NULL,
				error_code = NULL,
				error_message = NULL,
				retryable = FALSE,
				finished_at = CURRENT_TIMESTAMP,
				updated_at = CURRENT_TIMESTAMP
			WHERE id = $1 AND status = 'processing' AND worker_id = $2
		`, requestID, workerID, value.ID)
	})
}

func insertRecapSnapshotTx(ctx context.Context, tx *sql.Tx, value *model.Recap) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO recaps (
			id,
			profile_id,
			year,
			profile_name,
			profile_avatar_url,
			schema_version,
			algorithm_version,
			feature_schema_version,
			activity_hash,
			generated_at,
			narrative_source,
			prompt_version,
			narrative_model,
			main_vertical_code,
			main_vertical_title,
			accent_token,
			share_available,
			explanation_available,
			feedback_available,
			summary_title,
			summary_text
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21
		)
	`,
		value.ID,
		value.ProfileID,
		value.Year,
		value.Profile.Name,
		value.Profile.AvatarURL,
		value.SchemaVersion,
		value.Generation.AlgorithmVersion,
		value.Generation.FeatureSchemaVersion,
		value.Generation.ActivityHash,
		value.Generation.GeneratedAt,
		value.Generation.Narrative.Source,
		value.Generation.Narrative.PromptVersion,
		value.Generation.Narrative.Model,
		value.Theme.MainDistrict.Code,
		value.Theme.MainDistrict.Title,
		value.Theme.AccentToken,
		value.Capabilities.ShareAvailable,
		value.Capabilities.ExplanationAvailable,
		value.Capabilities.FeedbackAvailable,
		value.Narrative.SummaryTitle,
		value.Narrative.SummaryText,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrRecapAlreadyExists
		}
		return err
	}

	if err := insertCards(ctx, tx, value); err != nil {
		return err
	}
	if err := insertMetrics(ctx, tx, value); err != nil {
		return err
	}
	if err := insertArchetype(ctx, tx, value); err != nil {
		return err
	}
	if err := insertAchievements(ctx, tx, value); err != nil {
		return err
	}
	if err := insertExplanation(ctx, tx, value); err != nil {
		return err
	}
	if err := insertShare(ctx, tx, value); err != nil {
		return err
	}
	return nil
}

func insertCards(ctx context.Context, tx *sql.Tx, value *model.Recap) error {
	for _, card := range value.Cards {
		data, err := json.Marshal(card.Data)
		if err != nil {
			return fmt.Errorf("marshal recap card %s: %w", card.ID, err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO recap_cards (
				recap_id,
				card_id,
				type,
				position,
				visibility,
				eyebrow,
				title,
				description,
				visual_kind,
				visual_asset_code,
				explainable,
				data
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`,
			value.ID,
			card.ID,
			card.Type,
			card.Position,
			card.Visibility,
			card.Eyebrow,
			card.Title,
			card.Description,
			card.Visual.Kind,
			card.Visual.AssetCode,
			card.Explainable,
			data,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func insertMetrics(ctx context.Context, tx *sql.Tx, value *model.Recap) error {
	for _, metric := range value.Metrics {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO recap_metrics (
				recap_id,
				metric_code,
				value,
				unit,
				secondary_label
			) VALUES ($1, $2, $3, $4, $5)
		`,
			value.ID,
			metric.MetricCode,
			metric.Value,
			nullIfEmpty(string(metric.Unit)),
			metric.SecondaryLabel,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func insertArchetype(ctx context.Context, tx *sql.Tx, value *model.Recap) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO recap_archetypes (recap_id, role_code, style_code)
		VALUES ($1, $2, $3)
	`, value.ID, value.Archetype.Role.Code, value.Archetype.Style.Code)
	return err
}

func insertAchievements(ctx context.Context, tx *sql.Tx, value *model.Recap) error {
	for position, achievement := range value.Achievements {
		var currentValue *float64
		convertedCurrent := float64(achievement.Value)
		currentValue = &convertedCurrent

		var nextThreshold *float64
		if achievement.NextLevelThreshold != nil {
			converted := float64(*achievement.NextLevelThreshold)
			nextThreshold = &converted
		}

		_, err := tx.ExecContext(ctx, `
			INSERT INTO recap_achievements (
				recap_id,
				achievement_code,
				level,
				position,
				title,
				description,
				icon_code,
				metric_code,
				current_value,
				next_level_threshold
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		`,
			value.ID,
			achievement.Code,
			achievement.Level,
			position,
			achievement.Title,
			achievement.Description,
			achievement.Icon,
			achievement.MetricCode,
			currentValue,
			nextThreshold,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func insertExplanation(ctx context.Context, tx *sql.Tx, value *model.Recap) error {
	if value.Explanation == nil {
		return nil
	}
	for position, decision := range value.Explanation.Decisions {
		var roleCode interface{}
		var styleCode interface{}
		var achievementCode interface{}
		switch decision.Kind {
		case recap.ExplanationArchetypeRole:
			roleCode = decision.Code
		case recap.ExplanationArchetypeStyle:
			styleCode = decision.Code
		case recap.ExplanationAchievement:
			achievementCode = decision.Code
		default:
			return fmt.Errorf("unsupported explanation kind %q", decision.Kind)
		}

		var explanationID string
		err := tx.QueryRowContext(ctx, `
			INSERT INTO recap_explanations (
				recap_id,
				card_id,
				position,
				kind,
				role_code,
				style_code,
				achievement_code,
				reason,
				rule_version
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING id
		`,
			value.ID,
			decision.CardID,
			position,
			decision.Kind,
			roleCode,
			styleCode,
			achievementCode,
			decision.Reason,
			decision.RuleVersion,
		).Scan(&explanationID)
		if err != nil {
			return err
		}

		for factPosition, fact := range decision.Facts {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO recap_rule_facts (
					explanation_id,
					position,
					metric_code,
					actual,
					operator,
					threshold,
					matched
				) VALUES ($1, $2, $3, $4, $5, $6, $7)
			`,
				explanationID,
				factPosition,
				fact.MetricCode,
				fact.Actual,
				fact.Operator,
				fact.Threshold,
				fact.Matched,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func insertShare(ctx context.Context, tx *sql.Tx, value *model.Recap) error {
	if value.Share == nil {
		return nil
	}
	share := value.Share
	_, err := tx.ExecContext(ctx, `
		INSERT INTO share_cards (
			recap_id,
			schema_version,
			profile_name,
			avatar_url,
			year,
			title,
			subtitle,
			main_vertical_code,
			main_vertical_title,
			visual_theme
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`,
		value.ID,
		share.SchemaVersion,
		share.ProfileName,
		share.AvatarURL,
		share.Year,
		share.Title,
		share.Subtitle,
		share.MainDistrict.Code,
		share.MainDistrict.Title,
		share.Visual.Theme,
	)
	if err != nil {
		return err
	}

	for position, fact := range share.Facts {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO share_facts (recap_id, position, kind, label, value)
			VALUES ($1, $2, $3, $4, $5)
		`, value.ID, position, fact.Kind, fact.Label, fact.Value)
		if err != nil {
			return err
		}
	}

	for position, achievement := range share.Achievements {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO share_achievements (
				recap_id,
				achievement_code,
				position,
				title,
				level,
				icon_code
			) VALUES ($1, $2, $3, $4, $5, $6)
		`,
			value.ID,
			achievement.Code,
			position,
			achievement.Title,
			achievement.Level,
			achievement.Icon,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) loadCards(ctx context.Context, value *model.Recap) error {
	rows, err := r.DB.DB.QueryContext(ctx, `
		SELECT
			card_id,
			type,
			position,
			visibility,
			eyebrow,
			title,
			description,
			visual_kind,
			visual_asset_code,
			explainable,
			data
		FROM recap_cards
		WHERE recap_id = $1
		ORDER BY position
	`, value.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	cards := make([]model.RecapCard, 0)
	for rows.Next() {
		var card model.RecapCard
		var eyebrow sql.NullString
		var description sql.NullString
		var assetCode sql.NullString
		var data []byte
		if err := rows.Scan(
			&card.ID,
			&card.Type,
			&card.Position,
			&card.Visibility,
			&eyebrow,
			&card.Title,
			&description,
			&card.Visual.Kind,
			&assetCode,
			&card.Explainable,
			&data,
		); err != nil {
			return err
		}
		card.Eyebrow = nullableStringPointer(eyebrow)
		card.Description = nullableStringPointer(description)
		card.Visual.AssetCode = nullableStringPointer(assetCode)
		if err := json.Unmarshal(data, &card.Data); err != nil {
			return fmt.Errorf("unmarshal recap card %s: %w", card.ID, err)
		}
		cards = append(cards, card)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	value.Cards = cards
	return nil
}

func (r *Repository) loadExplanation(ctx context.Context, value *model.Recap) (*model.RecapExplanation, error) {
	rows, err := r.DB.DB.QueryContext(ctx, `
		SELECT
			id,
			card_id,
			kind,
			COALESCE(role_code, style_code, achievement_code),
			reason,
			rule_version
		FROM recap_explanations
		WHERE recap_id = $1
		ORDER BY position
	`, value.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	decisions := make([]model.DecisionExplanation, 0)
	for rows.Next() {
		var explanationID string
		var decision model.DecisionExplanation
		if err := rows.Scan(
			&explanationID,
			&decision.CardID,
			&decision.Kind,
			&decision.Code,
			&decision.Reason,
			&decision.RuleVersion,
		); err != nil {
			return nil, err
		}

		facts, err := r.loadRuleFacts(ctx, explanationID)
		if err != nil {
			return nil, err
		}
		decision.Facts = facts
		decisions = append(decisions, decision)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(decisions) == 0 {
		return nil, nil
	}
	return &model.RecapExplanation{
		RecapID:          value.ID.String(),
		AlgorithmVersion: value.Generation.AlgorithmVersion,
		ActivityHash:     value.Generation.ActivityHash,
		Decisions:        decisions,
	}, nil
}

func (r *Repository) loadRuleFacts(ctx context.Context, explanationID string) ([]model.RuleFact, error) {
	rows, err := r.DB.DB.QueryContext(ctx, `
		SELECT metric_code, actual, operator, threshold, matched
		FROM recap_rule_facts
		WHERE explanation_id = $1
		ORDER BY position
	`, explanationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	facts := make([]model.RuleFact, 0)
	for rows.Next() {
		var fact model.RuleFact
		if err := rows.Scan(
			&fact.MetricCode,
			&fact.Actual,
			&fact.Operator,
			&fact.Threshold,
			&fact.Matched,
		); err != nil {
			return nil, err
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}

func (r *Repository) loadShare(ctx context.Context, recapID string) (*model.ShareCard, error) {
	var share model.ShareCard
	var avatarURL sql.NullString
	err := r.DB.DB.QueryRowContext(ctx, `
		SELECT
			schema_version,
			profile_name,
			avatar_url,
			year,
			title,
			subtitle,
			main_vertical_code,
			main_vertical_title,
			visual_theme
		FROM share_cards
		WHERE recap_id = $1
	`, recapID).Scan(
		&share.SchemaVersion,
		&share.ProfileName,
		&avatarURL,
		&share.Year,
		&share.Title,
		&share.Subtitle,
		&share.MainDistrict.Code,
		&share.MainDistrict.Title,
		&share.Visual.Theme,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	share.RecapID = recapID
	share.AvatarURL = nullableStringPointer(avatarURL)

	facts, err := r.loadShareFacts(ctx, recapID)
	if err != nil {
		return nil, err
	}
	share.Facts = facts

	achievements, err := r.loadShareAchievements(ctx, recapID)
	if err != nil {
		return nil, err
	}
	share.Achievements = achievements
	return &share, nil
}

func (r *Repository) loadShareFacts(ctx context.Context, recapID string) ([]model.ShareFact, error) {
	rows, err := r.DB.DB.QueryContext(ctx, `
		SELECT kind, label, value
		FROM share_facts
		WHERE recap_id = $1
		ORDER BY position
	`, recapID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	facts := make([]model.ShareFact, 0)
	for rows.Next() {
		var fact model.ShareFact
		if err := rows.Scan(&fact.Kind, &fact.Label, &fact.Value); err != nil {
			return nil, err
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}

func (r *Repository) loadShareAchievements(ctx context.Context, recapID string) ([]model.ShareAchievement, error) {
	rows, err := r.DB.DB.QueryContext(ctx, `
		SELECT achievement_code, title, level, icon_code
		FROM share_achievements
		WHERE recap_id = $1
		ORDER BY position
	`, recapID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	achievements := make([]model.ShareAchievement, 0)
	for rows.Next() {
		var achievement model.ShareAchievement
		if err := rows.Scan(
			&achievement.Code,
			&achievement.Title,
			&achievement.Level,
			&achievement.Icon,
		); err != nil {
			return nil, err
		}
		achievements = append(achievements, achievement)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return achievements, nil
}

func nullableStringPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func nullIfEmpty(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

func isUniqueViolation(err error) bool {
	var pqError *pq.Error
	return errors.As(err, &pqError) && pqError.Code == "23505"
}
