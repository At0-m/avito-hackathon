package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/model"
	recap "recap-personalization/internal/recap"
	"recap-personalization/pkg/database"
)

const integrationYear = 2026

func TestIntegrationConcurrentRequestClaimsHaveNoDuplicates(t *testing.T) {
	repo := integrationRepository(t)

	profileID := createIntegrationProfile(t, repo, "claim")
	const requests = 32
	requestIDs := make(map[uuid.UUID]struct{}, requests)
	for index := 0; index < requests; index++ {
		value := createIntegrationRequest(t, repo, profileID, fmt.Sprintf("concurrency-%02d", index))
		requestIDs[value.ID] = struct{}{}
	}

	const workers = 8
	start := make(chan struct{})
	claimed := make(chan *model.RecapRequest, requests)
	errorsCh := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		workerID := fmt.Sprintf("integration-worker-%d", index)
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for {
				value, err := repo.ClaimNextRecapRequest(context.Background(), workerID, time.Minute)
				if errors.Is(err, ErrRecapRequestNotFound) {
					return
				}
				if err != nil {
					errorsCh <- err
					return
				}
				claimed <- value
			}
		}()
	}
	close(start)
	group.Wait()
	close(errorsCh)
	close(claimed)

	for err := range errorsCh {
		t.Fatalf("claim request: %v", err)
	}

	seen := make(map[uuid.UUID]int, requests)
	for value := range claimed {
		if _, ok := requestIDs[value.ID]; !ok {
			t.Fatalf("claimed unexpected request %s", value.ID)
		}
		seen[value.ID]++
		if value.AttemptCount != 1 {
			t.Errorf("request %s attempt_count=%d, want 1", value.ID, value.AttemptCount)
		}
	}
	if len(seen) != requests {
		t.Fatalf("claimed %d unique requests, want %d", len(seen), requests)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("request %s claimed %d times", id, count)
		}
	}
}

func TestIntegrationExpiredLeaseCanBeRecoveredAndStaleWorkerIsRejected(t *testing.T) {
	repo := integrationRepository(t)

	profileID := createIntegrationProfile(t, repo, "lease")
	request := createIntegrationRequest(t, repo, profileID, "lease-recovery")

	first, err := repo.ClaimRecapRequestByID(context.Background(), request.ID.String(), "worker-a", 150*time.Millisecond)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if first.AttemptCount != 1 {
		t.Fatalf("first attempt=%d, want 1", first.AttemptCount)
	}

	if _, err := repo.ClaimRecapRequestByID(context.Background(), request.ID.String(), "worker-b", time.Second); !errors.Is(err, ErrRecapRequestNotFound) {
		t.Fatalf("claim before lease expiry error=%v, want ErrRecapRequestNotFound", err)
	}

	time.Sleep(220 * time.Millisecond)
	second, err := repo.ClaimRecapRequestByID(context.Background(), request.ID.String(), "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("recovery claim: %v", err)
	}
	if second.AttemptCount != 2 {
		t.Fatalf("recovery attempt=%d, want 2", second.AttemptCount)
	}
	if second.WorkerID == nil || *second.WorkerID != "worker-b" {
		t.Fatalf("recovery worker=%v, want worker-b", second.WorkerID)
	}

	err = repo.UpdateRecapRequestProgress(context.Background(), request.ID.String(), "worker-a", "stale", 50)
	if !errors.Is(err, ErrRecapRequestLeaseLost) {
		t.Fatalf("stale worker update error=%v, want ErrRecapRequestLeaseLost", err)
	}
	if err := repo.MarkRecapRequestFailed(context.Background(), request.ID.String(), "worker-b", "integration_cleanup", "done", false); err != nil {
		t.Fatalf("finish recovered request: %v", err)
	}
}

func TestIntegrationFinalizeIsAtomicAndRequiresLeaseOwnership(t *testing.T) {
	repo := integrationRepository(t)
	profileID := createIntegrationProfile(t, repo, "finalize")
	request := createIntegrationRequest(t, repo, profileID, "finalize")

	if _, err := repo.ClaimRecapRequestByID(context.Background(), request.ID.String(), "worker-a", time.Minute); err != nil {
		t.Fatalf("claim request: %v", err)
	}

	value := integrationRecap(request, recap.ArchetypeRoleCode("missing-role"))
	if err := repo.FinalizeRecapRequest(context.Background(), request.ID.String(), "worker-a", value); err == nil {
		t.Fatal("expected invalid snapshot to roll back")
	}

	var snapshotCount int
	var requestStatus string
	if err := repo.DB.DB.QueryRowContext(context.Background(), `
		SELECT
			(SELECT count(*) FROM recaps WHERE id = $1),
			(SELECT status::text FROM recap_requests WHERE id = $1)
	`, request.ID).Scan(&snapshotCount, &requestStatus); err != nil {
		t.Fatalf("read rolled back finalize state: %v", err)
	}
	if snapshotCount != 0 || requestStatus != "processing" {
		t.Fatalf("after rollback snapshot_count=%d status=%q, want 0/processing", snapshotCount, requestStatus)
	}

	value.Archetype.Role = recap.ArchetypeRole{Code: recap.RoleShowcaseOwner, Title: "Хозяин витрины"}
	if err := repo.FinalizeRecapRequest(context.Background(), request.ID.String(), "worker-b", value); !errors.Is(err, ErrRecapRequestLeaseLost) {
		t.Fatalf("stale finalize error=%v, want ErrRecapRequestLeaseLost", err)
	}
	if err := repo.FinalizeRecapRequest(context.Background(), request.ID.String(), "worker-a", value); err != nil {
		t.Fatalf("finalize owned request: %v", err)
	}

	var recapID uuid.UUID
	if err := repo.DB.DB.QueryRowContext(context.Background(), `
		SELECT status::text, recap_id FROM recap_requests WHERE id = $1
	`, request.ID).Scan(&requestStatus, &recapID); err != nil {
		t.Fatalf("read finalized request: %v", err)
	}
	if requestStatus != "ready" || recapID != request.ID {
		t.Fatalf("finalized status=%q recap_id=%s, want ready/%s", requestStatus, recapID, request.ID)
	}
	if err := repo.DB.DB.QueryRowContext(context.Background(), `SELECT count(*) FROM recaps WHERE id = $1`, request.ID).Scan(&snapshotCount); err != nil {
		t.Fatalf("count finalized snapshots: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("snapshot_count=%d, want 1", snapshotCount)
	}
}

func TestIntegrationRestartedFailedRequestEmitsANewCommand(t *testing.T) {
	repo := integrationRepository(t)
	profileID := createIntegrationProfile(t, repo, "restart")
	request := createIntegrationRequest(t, repo, profileID, "restart-command")

	if _, err := repo.ClaimRecapRequestByID(context.Background(), request.ID.String(), "worker-a", time.Minute); err != nil {
		t.Fatalf("claim request: %v", err)
	}
	if err := repo.MarkRecapRequestFailed(context.Background(), request.ID.String(), "worker-a", "dependency_unavailable", "temporary", true); err != nil {
		t.Fatalf("mark request failed: %v", err)
	}
	if _, err := repo.RestartFailedRecapRequest(context.Background(), request.ID.String()); err != nil {
		t.Fatalf("restart request: %v", err)
	}

	rows, err := repo.DB.DB.QueryContext(context.Background(), `
		SELECT payload->>'command_id'
		FROM outbox_events
		WHERE aggregate_id = $1 AND event_type = 'GenerateRecapCommandV1'
		ORDER BY created_at, id
	`, request.ID)
	if err != nil {
		t.Fatalf("query commands: %v", err)
	}
	defer rows.Close()
	commands := make(map[string]struct{})
	for rows.Next() {
		var commandID string
		if err := rows.Scan(&commandID); err != nil {
			t.Fatalf("scan command id: %v", err)
		}
		commands[commandID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate commands: %v", err)
	}
	if len(commands) != 2 {
		t.Fatalf("unique command ids=%d, want 2", len(commands))
	}
}

func TestIntegrationRetryRequeueCreatesANewCommandAtomically(t *testing.T) {
	repo := integrationRepository(t)
	profileID := createIntegrationProfile(t, repo, "retry-command")
	request := createIntegrationRequest(t, repo, profileID, "retry-command")

	if _, err := repo.ClaimRecapRequestByID(context.Background(), request.ID.String(), "worker-a", time.Minute); err != nil {
		t.Fatalf("claim request: %v", err)
	}
	if err := repo.RequeueRecapRequest(
		context.Background(),
		request.ID.String(),
		"worker-a",
		0,
		"dependency_unavailable",
		"temporary",
	); err != nil {
		t.Fatalf("requeue request: %v", err)
	}

	var status string
	var commandCount int
	if err := repo.DB.DB.QueryRowContext(context.Background(), `
		SELECT
			(SELECT status::text FROM recap_requests WHERE id = $1),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'GenerateRecapCommandV1')
	`, request.ID).Scan(&status, &commandCount); err != nil {
		t.Fatalf("read retry state: %v", err)
	}
	if status != "queued" {
		t.Fatalf("status=%q, want queued", status)
	}
	if commandCount != 2 {
		t.Fatalf("generation commands=%d, want 2", commandCount)
	}
}

func TestIntegrationOutboxClaimsAndInboxDedupAreConcurrentSafe(t *testing.T) {
	repo := integrationRepository(t)

	profileID := createIntegrationProfile(t, repo, "eventing")
	const events = 24
	for index := 0; index < events; index++ {
		createIntegrationRequest(t, repo, profileID, fmt.Sprintf("outbox-%02d", index))
	}

	start := make(chan struct{})
	claimed := make(chan model.OutboxEvent, events)
	errorsCh := make(chan error, 4)
	var group sync.WaitGroup
	for index := 0; index < 4; index++ {
		publisherID := fmt.Sprintf("publisher-%d", index)
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for {
				batch, err := repo.ClaimOutboxEvents(context.Background(), publisherID, 3, time.Minute)
				if err != nil {
					errorsCh <- err
					return
				}
				if len(batch) == 0 {
					return
				}
				for _, value := range batch {
					claimed <- value
				}
			}
		}()
	}
	close(start)
	group.Wait()
	close(errorsCh)
	close(claimed)
	for err := range errorsCh {
		t.Fatalf("claim outbox: %v", err)
	}

	seen := make(map[uuid.UUID]int, events)
	var recoveryCandidate model.OutboxEvent
	for value := range claimed {
		seen[value.ID]++
		if recoveryCandidate.ID == uuid.Nil {
			recoveryCandidate = value
		}
	}
	if len(seen) != events {
		t.Fatalf("claimed %d unique outbox events, want %d", len(seen), events)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("outbox event %s claimed %d times", id, count)
		}
	}

	if _, err := repo.DB.DB.ExecContext(context.Background(), `
		UPDATE outbox_events SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = $1
	`, recoveryCandidate.ID); err != nil {
		t.Fatalf("expire outbox lease: %v", err)
	}
	recovered, err := repo.ClaimOutboxEvents(context.Background(), "recovery-publisher", 1, time.Minute)
	if err != nil {
		t.Fatalf("recover expired outbox event: %v", err)
	}
	if len(recovered) != 1 || recovered[0].ID != recoveryCandidate.ID {
		t.Fatalf("recovered events=%v, want %s", recovered, recoveryCandidate.ID)
	}
	if recovered[0].AttemptCount != 2 {
		t.Fatalf("recovered attempt_count=%d, want 2", recovered[0].AttemptCount)
	}

	eventID := uuid.New()
	inserted := make(chan bool, 20)
	inboxErrors := make(chan error, 20)
	group = sync.WaitGroup{}
	for index := 0; index < 20; index++ {
		group.Add(1)
		go func(offset int64) {
			defer group.Done()
			value, err := repo.RecordConsumedEvent(context.Background(), "integration-consumer", eventID, "topic", 0, offset)
			if err != nil {
				inboxErrors <- err
				return
			}
			inserted <- value
		}(int64(index))
	}
	group.Wait()
	close(inserted)
	close(inboxErrors)
	for err := range inboxErrors {
		t.Fatalf("record inbox event: %v", err)
	}
	trueCount := 0
	for value := range inserted {
		if value {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Fatalf("inbox inserted count=%d, want 1", trueCount)
	}
}

func integrationRepository(t *testing.T) *Repository {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db, err := database.NewPostgresDB(url)
	if err != nil {
		t.Fatalf("connect integration database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewRepository(db)
}

func createIntegrationProfile(t *testing.T, repo *Repository, suffix string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	ctx := context.Background()
	if _, err := repo.DB.DB.ExecContext(ctx, `
		INSERT INTO profiles (id, name, description, scenario)
		VALUES ($1, $2, $3, 'integration')
	`, id, "Integration "+suffix, "Temporary integration-test profile"); err != nil {
		t.Fatalf("insert profile: %v", err)
	}
	if _, err := repo.DB.DB.ExecContext(ctx, `
		INSERT INTO profile_available_years (profile_id, year) VALUES ($1, $2)
	`, id, integrationYear); err != nil {
		t.Fatalf("insert profile year: %v", err)
	}
	t.Cleanup(func() {
		_, _ = repo.DB.DB.ExecContext(context.Background(), `DELETE FROM consumed_events WHERE consumer_name = 'integration-consumer'`)
		_, _ = repo.DB.DB.ExecContext(context.Background(), `DELETE FROM outbox_events WHERE aggregate_id IN (SELECT id FROM recap_requests WHERE profile_id = $1)`, id)
		_, _ = repo.DB.DB.ExecContext(context.Background(), `DELETE FROM recap_requests WHERE profile_id = $1`, id)
		_, _ = repo.DB.DB.ExecContext(context.Background(), `DELETE FROM recaps WHERE profile_id = $1`, id)
		_, _ = repo.DB.DB.ExecContext(context.Background(), `DELETE FROM profile_available_years WHERE profile_id = $1`, id)
		_, _ = repo.DB.DB.ExecContext(context.Background(), `DELETE FROM profiles WHERE id = $1`, id)
	})
	return id
}

func integrationRecap(request *model.RecapRequest, roleCode recap.ArchetypeRoleCode) *model.Recap {
	now := time.Now().UTC()
	return &model.Recap{
		SchemaVersion: "2.0",
		ID:            request.ID,
		ProfileID:     request.ProfileID,
		Year:          request.Year,
		Profile: model.RecapProfile{
			ID:   request.ProfileID,
			Name: "Integration profile",
		},
		Generation: model.RecapGeneration{
			AlgorithmVersion:     request.AlgorithmVersion,
			FeatureSchemaVersion: "integration-v1",
			ActivityHash:         "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			GeneratedAt:          now,
			Narrative: model.NarrativeGeneration{
				Source:        "template",
				PromptVersion: "integration-v1",
			},
		},
		Theme: model.RecapTheme{
			Code: "city",
			MainDistrict: model.Vertical{
				Code:  "goods",
				Title: "Товары",
			},
		},
		Archetype: recap.ArchetypeDecision{
			Role:  recap.ArchetypeRole{Code: roleCode, Title: "Integration role"},
			Style: recap.ArchetypeStyle{Code: recap.StyleResultOriented, Title: "Результативный"},
		},
		Narrative: recap.Narrative{
			SummaryTitle:  "Integration recap",
			SummaryText:   "Transactional finalize integration test.",
			Source:        "template",
			PromptVersion: "integration-v1",
		},
	}
}

func createIntegrationRequest(t *testing.T, repo *Repository, profileID uuid.UUID, suffix string) *model.RecapRequest {
	t.Helper()
	now := time.Now().UTC()
	value := &model.RecapRequest{
		ID:               uuid.New(),
		ProfileID:        profileID,
		Year:             integrationYear,
		Status:           model.RecapRequestQueued,
		Stage:            "queued",
		AlgorithmVersion: "integration-" + suffix,
		IdempotencyKey:   profileID.String() + ":" + suffix,
		MaxAttempts:      3,
		AvailableAt:      now,
	}
	stored, created, err := repo.CreateOrGetRecapRequest(context.Background(), value)
	if err != nil {
		t.Fatalf("create request %s: %v", suffix, err)
	}
	if !created {
		t.Fatalf("request %s unexpectedly existed", suffix)
	}
	return stored
}
