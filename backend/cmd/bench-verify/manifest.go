package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Entry is one MANIFEST line written by bench/k6/recap.js.
type Entry struct {
	Scenario  string  `json:"s"`
	Phase     string  `json:"ph"`
	Iteration int64   `json:"it"`
	Index     int     `json:"pi"`
	ProfileID string  `json:"p"`
	Year      int     `json:"y"`
	StartMS   int64   `json:"t"`
	DurMS     float64 `json:"d"`
	Status    int     `json:"c"`
	ID        string  `json:"id"`
	Kind      string  `json:"k"`
	Reason    string  `json:"r"`
	User      bool    `json:"u"`
	Outcome   string  `json:"o"`
	ReadyMS   float64 `json:"tr"`
	Polls     int     `json:"n"`
}

// Manifest kinds.
const (
	KindAccepted      = "accepted"
	KindSnapshot      = "snapshot"
	KindFailed202     = "failed_202"
	KindContractError = "contract_error"
	KindRejected      = "rejected"
	KindTransport     = "transport"
	// KindExhausted: a planned POST had no seeded profile (load not reached).
	KindExhausted = "exhausted"
	// KindOverQuota: the executor started an iteration after the quota was
	// complete; no POST was sent and it is not an error.
	KindOverQuota = "over_quota"
)

type manifestStats struct {
	Files       []string `json:"files"`
	Lines       int      `json:"manifest_lines"`
	ParseErrors int      `json:"parse_errors"`
}

const manifestMarker = "MANIFEST "

// parseManifestLine accepts k6 JSON log lines ({"msg":"MANIFEST {...}"}) and
// raw console lines that contain the marker.
func parseManifestLine(line string) (Entry, bool, error) {
	if !strings.Contains(line, "MANIFEST") {
		return Entry{}, false, nil
	}
	payload := ""
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "{") {
		var logLine struct {
			Msg string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(trimmed), &logLine); err == nil && strings.HasPrefix(logLine.Msg, manifestMarker) {
			payload = strings.TrimPrefix(logLine.Msg, manifestMarker)
		}
	}
	if payload == "" {
		index := strings.Index(line, manifestMarker)
		if index < 0 {
			return Entry{}, false, nil
		}
		payload = strings.TrimSpace(line[index+len(manifestMarker):])
	}
	var entry Entry
	if err := json.Unmarshal([]byte(payload), &entry); err != nil {
		return Entry{}, true, err
	}
	return entry, true, nil
}

func readManifests(paths []string, scenarios map[string]bool) ([]Entry, manifestStats, error) {
	stats := manifestStats{Files: paths}
	var entries []Entry
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, stats, fmt.Errorf("open manifest %s: %w", path, err)
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 1<<20), 16<<20)
		for scanner.Scan() {
			entry, ok, err := parseManifestLine(scanner.Text())
			if !ok {
				continue
			}
			if err != nil {
				stats.ParseErrors++
				continue
			}
			if len(scenarios) > 0 && !scenarios[entry.Scenario] {
				continue
			}
			stats.Lines++
			entries = append(entries, entry)
		}
		scanErr := scanner.Err()
		_ = file.Close()
		if scanErr != nil {
			return nil, stats, fmt.Errorf("read manifest %s: %w", path, scanErr)
		}
	}
	return entries, stats, nil
}

// acceptedIDs returns unique IDs confirmed by a valid 202/200 response.
func acceptedIDs(entries []Entry) []string {
	seen := make(map[string]struct{})
	var ids []string
	for _, entry := range entries {
		switch entry.Kind {
		case KindAccepted, KindFailed202, KindSnapshot:
		default:
			continue
		}
		if entry.ID == "" {
			continue
		}
		if _, ok := seen[entry.ID]; ok {
			continue
		}
		seen[entry.ID] = struct{}{}
		ids = append(ids, entry.ID)
	}
	return ids
}

// manifestStart returns the start time of the first POST in the manifests.
// constant-arrival-rate starts its first iteration at t=0, so this is the
// scenario start within client clock precision.
func manifestStart(paths []string) (time.Time, error) {
	entries, _, err := readManifests(paths, nil)
	if err != nil {
		return time.Time{}, err
	}
	var first int64
	for _, entry := range entries {
		if entry.StartMS > 0 && (first == 0 || entry.StartMS < first) {
			first = entry.StartMS
		}
	}
	if first == 0 {
		return time.Time{}, fmt.Errorf("manifest has no POST timestamps")
	}
	return time.UnixMilli(first).UTC(), nil
}

func profileIDs(entries []Entry) []string {
	seen := make(map[string]struct{})
	var ids []string
	for _, entry := range entries {
		if entry.ProfileID == "" {
			continue
		}
		if _, ok := seen[entry.ProfileID]; ok {
			continue
		}
		seen[entry.ProfileID] = struct{}{}
		ids = append(ids, entry.ProfileID)
	}
	return ids
}
