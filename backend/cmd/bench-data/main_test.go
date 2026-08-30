package main

import (
	"testing"
)

func TestEventTypeDistributionBuckets(t *testing.T) {
	counts := map[string]int{}
	for value := 0; value < 100; value++ {
		counts[eventType(value)]++
	}

	if got := counts["listing_viewed"] + counts["search_saved"]; got != 70 {
		t.Fatalf("views/search share=%d, want 70", got)
	}
	if got := counts["favorite_added"]; got != 15 {
		t.Fatalf("favorites share=%d, want 15", got)
	}
	if got := counts["chat_started"]; got != 7 {
		t.Fatalf("chats share=%d, want 7", got)
	}
	if got := counts["listing_published"] + counts["sale_completed"] + counts["purchase_completed"]; got != 5 {
		t.Fatalf("publish/sale share=%d, want 5", got)
	}
	if got := counts["delivery_used"]; got != 3 {
		t.Fatalf("delivery/other share=%d, want 3", got)
	}
}

func TestBenchmarkProfileIDIsDeterministicAndIndexSpecific(t *testing.T) {
	first := benchmarkProfileID(42, 0)
	if first != benchmarkProfileID(42, 0) {
		t.Fatal("same seed and index must produce the same profile ID")
	}
	if first == benchmarkProfileID(42, 1) {
		t.Fatal("different indexes must produce different profile IDs")
	}
	if first == benchmarkProfileID(43, 0) {
		t.Fatal("different seeds must produce different profile IDs")
	}
}
