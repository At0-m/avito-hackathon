package config

import "testing"

func TestLoadDefaultsWorkerCommandSourceToHybrid(t *testing.T) {
	t.Setenv("WORKER_COMMAND_SOURCE", "")
	value := Load()
	if value.WorkerCommandSource != "hybrid" {
		t.Fatalf("WorkerCommandSource=%q, want hybrid", value.WorkerCommandSource)
	}
}

func TestLoadNormalizesWorkerCommandSource(t *testing.T) {
	t.Setenv("WORKER_COMMAND_SOURCE", "HYBRID")
	value := Load()
	if value.WorkerCommandSource != "hybrid" {
		t.Fatalf("WorkerCommandSource=%q, want hybrid", value.WorkerCommandSource)
	}
}
