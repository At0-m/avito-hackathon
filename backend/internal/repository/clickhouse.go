package repository

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"recap-personalization/internal/recap/ports"
	"recap-personalization/pkg/database"
)

type ClickHouseRepository struct {
	DB *database.ClickHouseHTTP
}

func NewClickHouseRepository(db *database.ClickHouseHTTP) *ClickHouseRepository {
	return &ClickHouseRepository{DB: db}
}

var (
	_ ports.ActivityRepository    = (*ClickHouseRepository)(nil)
	_ ports.InteractionRepository = (*ClickHouseRepository)(nil)
)

func (r *ClickHouseRepository) GetActivitiesByProfileIDAndYear(
	ctx context.Context,
	profileID string,
	year int,
) ([]ports.ActivityEvent, error) {
	if r == nil || r.DB == nil {
		return nil, fmt.Errorf("clickhouse repository is not configured")
	}
	query := fmt.Sprintf(`
		SELECT
			toString(event_id) AS event_id,
			toString(profile_id) AS profile_id,
			event_type,
			vertical_code,
			category_code,
			toString(toUnixTimestamp64Milli(occurred_at)) AS occurred_at_ms
		FROM activity_events FINAL
		WHERE toString(profile_id) = '%s'
			AND occurred_at >= toDateTime64('%04d-01-01 00:00:00', 3, 'UTC')
    	AND occurred_at < toDateTime64('%04d-01-01 00:00:00', 3, 'UTC')
		ORDER BY occurred_at, event_id
		FORMAT JSONEachRow
	`, profileID, year, year+1)
	body, err := r.DB.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	type row struct {
		EventID      string `json:"event_id"`
		ProfileID    string `json:"profile_id"`
		EventType    string `json:"event_type"`
		VerticalCode string `json:"vertical_code"`
		CategoryCode string `json:"category_code"`
		OccurredAtMS string `json:"occurred_at_ms"`
	}

	result := make([]ports.ActivityEvent, 0)
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var value row
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return nil, fmt.Errorf("decode activity event: %w", err)
		}
		occurredAtMS, err := strconv.ParseInt(value.OccurredAtMS, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse activity occurred_at_ms: %w", err)
		}
		result = append(result, ports.ActivityEvent{
			EventID:      value.EventID,
			ProfileID:    value.ProfileID,
			EventType:    value.EventType,
			VerticalCode: value.VerticalCode,
			CategoryCode: value.CategoryCode,
			OccurredAt:   time.UnixMilli(occurredAtMS).UTC(),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *ClickHouseRepository) SaveInteraction(ctx context.Context, event ports.InteractionEvent) error {
	if r == nil || r.DB == nil {
		return fmt.Errorf("clickhouse repository is not configured")
	}
	exists, err := r.interactionExists(ctx, event.EventID)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	properties, err := json.Marshal(event.Properties)
	if err != nil {
		return fmt.Errorf("marshal interaction properties: %w", err)
	}
	row := map[string]interface{}{
		"event_id":    event.EventID,
		"recap_id":    event.RecapID,
		"session_id":  event.SessionID,
		"event_name":  event.EventName,
		"occurred_at": event.OccurredAt.UTC().Format("2006-01-02 15:04:05.000"),
		"properties":  string(properties),
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshal interaction row: %w", err)
	}
	_, err = r.DB.Execute(ctx, "INSERT INTO interactions FORMAT JSONEachRow\n"+string(encoded)+"\n")
	return err
}

func (r *ClickHouseRepository) GetInteractionsByRecapID(
	ctx context.Context,
	recapID string,
) ([]ports.InteractionEvent, error) {
	if r == nil || r.DB == nil {
		return nil, fmt.Errorf("clickhouse repository is not configured")
	}
	query := fmt.Sprintf(`
		SELECT
			toString(event_id) AS event_id,
			toString(recap_id) AS recap_id,
			toString(session_id) AS session_id,
			event_name,
			toString(toUnixTimestamp64Milli(occurred_at)) AS occurred_at_ms,
			properties
		FROM interactions FINAL
		WHERE toString(recap_id) = '%s'
		ORDER BY occurred_at, event_id
		FORMAT JSONEachRow
	`, recapID)
	body, err := r.DB.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	type row struct {
		EventID      string `json:"event_id"`
		RecapID      string `json:"recap_id"`
		SessionID    string `json:"session_id"`
		EventName    string `json:"event_name"`
		OccurredAtMS string `json:"occurred_at_ms"`
		Properties   string `json:"properties"`
	}

	result := make([]ports.InteractionEvent, 0)
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var value row
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return nil, fmt.Errorf("decode interaction event: %w", err)
		}
		properties := make(map[string]interface{})
		if value.Properties != "" {
			if err := json.Unmarshal([]byte(value.Properties), &properties); err != nil {
				return nil, fmt.Errorf("decode interaction properties: %w", err)
			}
		}

		occurredAtMS, err := strconv.ParseInt(value.OccurredAtMS, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse interaction occurred_at_ms: %w", err)
		}
		result = append(result, ports.InteractionEvent{
			EventID:    value.EventID,
			RecapID:    value.RecapID,
			SessionID:  value.SessionID,
			EventName:  value.EventName,
			OccurredAt: time.UnixMilli(occurredAtMS).UTC(),
			Properties: properties,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *ClickHouseRepository) interactionExists(ctx context.Context, eventID string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT count() AS count
		FROM interactions FINAL
		WHERE toString(event_id) = '%s'
		FORMAT JSONEachRow
	`, eventID)
	body, err := r.DB.Execute(ctx, query)
	if err != nil {
		return false, err
	}
	var result struct {
		Count interface{} `json:"count"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(body))), &result); err != nil {
		return false, fmt.Errorf("decode interaction count: %w", err)
	}
	switch value := result.Count.(type) {
	case float64:
		return value > 0, nil
	case string:
		count, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return false, err
		}
		return count > 0, nil
	default:
		return false, fmt.Errorf("unexpected interaction count type %T", result.Count)
	}
}
