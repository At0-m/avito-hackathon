package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type eventRow struct {
	EventID      string `json:"event_id"`
	ProfileID    string `json:"profile_id"`
	EventType    string `json:"event_type"`
	VerticalCode string `json:"vertical_code"`
	CategoryCode string `json:"category_code"`
	OccurredAt   string `json:"occurred_at"`
}

type category struct {
	vertical string
	code     string
}

var categories = []category{
	{vertical: "goods", code: "electronics"},
	{vertical: "goods", code: "home_and_garden"},
	{vertical: "goods", code: "clothing_and_accessories"},
	{vertical: "goods", code: "hobbies_and_leisure"},
	{vertical: "transport", code: "cars"},
	{vertical: "real_estate", code: "apartments"},
	{vertical: "jobs", code: "vacancies"},
	{vertical: "services", code: "personal_services"},
}

func main() {
	profiles := flag.Int("profiles", 100, "number of benchmark profiles")
	eventsPerProfile := flag.Int("events-per-profile", 50, "events generated for each profile")
	year := flag.Int("year", 2026, "activity year")
	seed := flag.Int64("seed", 42, "deterministic dataset seed")
	batchSize := flag.Int("batch-size", 10000, "ClickHouse rows per insert")
	postgresURL := flag.String("postgres-url", env("DATABASE_URL", "postgres://avito:avito_password@localhost:5432/avito_recap?sslmode=disable"), "PostgreSQL connection URL")
	clickhouseURL := flag.String("clickhouse-url", env("CLICKHOUSE_URL", "http://localhost:8123"), "ClickHouse HTTP endpoint")
	flag.Parse()

	if *profiles <= 0 || *eventsPerProfile <= 0 || *batchSize <= 0 {
		log.Fatal("profiles, events-per-profile and batch-size must be positive")
	}

	ctx := context.Background()
	db, err := sql.Open("postgres", *postgresURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	ids := make([]uuid.UUID, *profiles)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	for index := 0; index < *profiles; index++ {
		id := benchmarkProfileID(*seed, index)
		ids[index] = id
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO profiles (id, name, description, scenario)
			VALUES ($1, $2, $3, 'benchmark')
			ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
		`, id, fmt.Sprintf("Benchmark %06d", index), "Synthetic benchmark profile"); err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO profile_available_years (profile_id, year)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, id, *year); err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	random := rand.New(rand.NewSource(*seed))
	rows := make([]eventRow, 0, *batchSize)
	total := 0
	flush := func() {
		if len(rows) == 0 {
			return
		}
		if err := insertClickHouse(ctx, *clickhouseURL, rows); err != nil {
			log.Fatal(err)
		}
		total += len(rows)
		rows = rows[:0]
		log.Printf("inserted %d events", total)
	}

	for profileIndex, profileID := range ids {
		for eventIndex := 0; eventIndex < *eventsPerProfile; eventIndex++ {
			item := categories[zipfIndex(random, len(categories))]
			occurred := time.Date(*year, 1, 1, 12, 0, 0, 0, time.UTC).
				Add(time.Duration((profileIndex+eventIndex)%330) * 24 * time.Hour).
				Add(time.Duration(eventIndex%24) * time.Minute)
			rows = append(rows, eventRow{
				EventID:      uuid.NewSHA1(profileID, []byte(fmt.Sprintf("event-%d", eventIndex))).String(),
				ProfileID:    profileID.String(),
				EventType:    eventType(random.Intn(100)),
				VerticalCode: item.vertical,
				CategoryCode: item.code,
				OccurredAt:   occurred.Format("2006-01-02 15:04:05.000"),
			})
			if len(rows) >= *batchSize {
				flush()
			}
		}
	}
	flush()
	log.Printf("benchmark dataset ready: profiles=%d events=%d year=%d seed=%d", *profiles, total, *year, *seed)
}

func benchmarkProfileID(seed int64, index int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("avito-recap-bench:%d:%d", seed, index)))
}

func eventType(value int) string {
	switch {
	case value < 60:
		return "listing_viewed"
	case value < 70:
		return "search_saved"
	case value < 85:
		return "favorite_added"
	case value < 92:
		return "chat_started"
	case value < 94:
		return "listing_published"
	case value < 96:
		return "sale_completed"
	case value < 97:
		return "purchase_completed"
	default:
		return "delivery_used"
	}
}

func zipfIndex(random *rand.Rand, length int) int {
	value := random.Float64()
	switch {
	case value < 0.40:
		return 0
	case value < 0.65:
		return min(1, length-1)
	case value < 0.80:
		return min(2, length-1)
	default:
		return random.Intn(length)
	}
}

func insertClickHouse(ctx context.Context, endpoint string, rows []eventRow) error {
	var body bytes.Buffer
	body.WriteString("INSERT INTO recap.activity_events FORMAT JSONEachRow\n")
	encoder := json.NewEncoder(&body)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/"), &body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "text/plain; charset=utf-8")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("ClickHouse insert returned HTTP %d", response.StatusCode)
	}
	return nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
