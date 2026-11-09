package config

import "testing"

func TestLoadDefaultsWorkerCommandSourceToHybrid(t *testing.T) {
	t.Setenv("WORKER_COMMAND_SOURCE", "")
	value := Load()
	if value.WorkerCommandSource != "hybrid" {
		t.Fatalf("WorkerCommandSource=%q, want hybrid", value.WorkerCommandSource)
	}
}

func TestNarrativeProviderMode(t *testing.T) {
	cases := []struct {
		name     string
		provider string
		key      string
		want     string
		wantErr  bool
	}{
		{name: "auto without key uses template", provider: "auto", want: NarrativeProviderDisabled},
		{name: "empty without key uses template", provider: "", want: NarrativeProviderDisabled},
		{name: "auto with key keeps Mistral", provider: "auto", key: "secret", want: NarrativeProviderMistral},
		{name: "disabled ignores a key from the host", provider: "disabled", key: "secret", want: NarrativeProviderDisabled},
		{name: "disabled is case insensitive", provider: " Disabled ", key: "secret", want: NarrativeProviderDisabled},
		{name: "explicit mistral requires key", provider: "mistral", wantErr: true},
		{name: "explicit mistral with key", provider: "mistral", key: "secret", want: NarrativeProviderMistral},
		{name: "unknown value is rejected", provider: "stub", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := (Config{NarrativeProvider: tc.provider, MistralAPIKey: tc.key}).NarrativeProviderMode()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got mode %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("mode=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestLoadNarrativeProviderDefaultsToAuto(t *testing.T) {
	t.Setenv("NARRATIVE_PROVIDER", "")
	t.Setenv("METRICS_ADDR", "")
	value := Load()
	if value.NarrativeProvider != NarrativeProviderAuto {
		t.Fatalf("NarrativeProvider=%q, want auto", value.NarrativeProvider)
	}
	if value.MetricsAddr != "" {
		t.Fatalf("MetricsAddr=%q, want disabled by default", value.MetricsAddr)
	}
}

func TestLoadNormalizesWorkerCommandSource(t *testing.T) {
	t.Setenv("WORKER_COMMAND_SOURCE", "HYBRID")
	value := Load()
	if value.WorkerCommandSource != "hybrid" {
		t.Fatalf("WorkerCommandSource=%q, want hybrid", value.WorkerCommandSource)
	}
}
