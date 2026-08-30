package handler

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"recap-personalization/internal/model"
	"recap-personalization/internal/service"
)

func TestMarshalRecapEventUsesStatusReadyAndFailedEvents(t *testing.T) {
	requestID := uuid.New()
	profileID := uuid.New()

	statusPayload, statusEvent, terminal, err := marshalRecapEvent(&service.RecapResult{
		Request: &model.RecapRequestStatusResponse{
			ID: requestID, ProfileID: profileID, Year: 2026,
			Status: model.RecapRequestProcessing, Stage: "computing_features", ProgressPercent: 35,
		},
	})
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	if statusEvent != "status" || terminal {
		t.Fatalf("status event=%q terminal=%v", statusEvent, terminal)
	}
	var status map[string]interface{}
	if err := json.Unmarshal(statusPayload, &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status["status"] != "processing" {
		t.Fatalf("status payload=%v", status)
	}

	readyPayload, readyEvent, terminal, err := marshalRecapEvent(&service.RecapResult{
		Recap: &model.Recap{SchemaVersion: "2.0", ID: requestID, ProfileID: profileID, Year: 2026},
	})
	if err != nil {
		t.Fatalf("marshal ready: %v", err)
	}
	if readyEvent != "ready" || !terminal {
		t.Fatalf("ready event=%q terminal=%v", readyEvent, terminal)
	}
	if !json.Valid(readyPayload) {
		t.Fatalf("ready payload is not JSON: %s", readyPayload)
	}

	failedPayload, failedEvent, terminal, err := marshalRecapEvent(&service.RecapResult{
		Request: &model.RecapRequestStatusResponse{
			ID: requestID, ProfileID: profileID, Year: 2026,
			Status: model.RecapRequestFailed, Stage: "failed", ProgressPercent: 35,
			Error: &model.RecapRequestError{Code: "dependency_unavailable", Message: "temporary", Retryable: true},
		},
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if failedEvent != "failed" || !terminal {
		t.Fatalf("failed event=%q terminal=%v", failedEvent, terminal)
	}
	if !json.Valid(failedPayload) {
		t.Fatalf("failed payload is not JSON: %s", failedPayload)
	}
}

func TestMarshalRecapEventRejectsEmptyResult(t *testing.T) {
	if _, _, _, err := marshalRecapEvent(nil); err == nil {
		t.Fatal("expected an error for nil result")
	}
	if _, _, _, err := marshalRecapEvent(&service.RecapResult{}); err == nil {
		t.Fatal("expected an error for empty result")
	}
}
