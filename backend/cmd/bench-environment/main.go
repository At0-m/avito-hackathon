package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type environment struct {
	CapturedAt           time.Time `json:"captured_at"`
	Hostname             string    `json:"hostname"`
	OperatingSystem      string    `json:"operating_system"`
	Architecture         string    `json:"architecture"`
	LogicalCPUs          int       `json:"logical_cpus"`
	TotalMemoryBytes     uint64    `json:"total_memory_bytes,omitempty"`
	GoVersion            string    `json:"go_version"`
	DockerVersion        string    `json:"docker_version,omitempty"`
	DockerComposeVersion string    `json:"docker_compose_version,omitempty"`
	Note                 string    `json:"note"`
	// Effective runtime values reported by the running processes through
	// recap_config_value / recap_narrative_provider_mode (bench stand only).
	EffectiveConfig map[string][]instanceValue `json:"effective_config,omitempty"`
	NarrativeModes  map[string][]string        `json:"narrative_provider_modes,omitempty"`
	ScrapeTargets   map[string]int             `json:"scrape_targets_up,omitempty"`
	MetricsError    string                     `json:"metrics_error,omitempty"`
	// Host-side facts captured by bench/run.sh (git SHA, docker versions,
	// container env allow-list with secrets redacted, image IDs).
	Host map[string]json.RawMessage `json:"host,omitempty"`
}

type instanceValue struct {
	Job      string  `json:"job"`
	Instance string  `json:"instance"`
	Value    float64 `json:"value"`
}

func main() {
	output := flag.String("output", "bench/results/environment.json", "output JSON path")
	vmURL := flag.String("vm-url", "", "optional VictoriaMetrics URL to read effective configuration exported by the processes")
	var hostFiles multiFlag
	flag.Var(&hostFiles, "host-file", "name=path of a JSON file captured on the host; may be repeated")
	flag.Parse()

	hostname, _ := os.Hostname()
	value := environment{
		CapturedAt:           time.Now().UTC(),
		Hostname:             hostname,
		OperatingSystem:      runtime.GOOS,
		Architecture:         runtime.GOARCH,
		LogicalCPUs:          runtime.NumCPU(),
		TotalMemoryBytes:     totalMemoryBytes(),
		GoVersion:            runtime.Version(),
		DockerVersion:        commandOutput("docker", "version", "--format", "{{.Server.Version}}"),
		DockerComposeVersion: commandOutput("docker", "compose", "version", "--short"),
		Note:                 "Record host CPU/RAM and Docker resource limits next to benchmark results when limits are configured outside Compose.",
	}
	if *vmURL != "" {
		if err := collectEffectiveConfig(*vmURL, &value); err != nil {
			value.MetricsError = err.Error()
		}
	}
	for _, item := range hostFiles {
		name, path, ok := strings.Cut(item, "=")
		if !ok {
			log.Fatalf("host-file %q must be name=path", item)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("skip host file %s: %v", path, err)
			continue
		}
		if !json.Valid(data) {
			quoted, _ := json.Marshal(string(data))
			data = quoted
		}
		if value.Host == nil {
			value.Host = make(map[string]json.RawMessage)
		}
		value.Host[name] = data
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*output, encoded, 0o644); err != nil {
		log.Fatal(err)
	}
}

func totalMemoryBytes() uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "MemTotal:" {
			continue
		}
		value, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			return 0
		}
		return value * 1024
	}
	return 0
}

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(value string) error {
	*m = append(*m, value)
	return nil
}

type vmVector struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]interface{}    `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func queryVM(baseURL, query string) (vmVector, error) {
	var value vmVector
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Get(strings.TrimRight(baseURL, "/") + "/api/v1/query?query=" + url.QueryEscape(query))
	if err != nil {
		return value, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return value, fmt.Errorf("query %q: HTTP %d", query, response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		return value, err
	}
	return value, nil
}

func collectEffectiveConfig(baseURL string, value *environment) error {
	configs, err := queryVM(baseURL, "recap_config_value")
	if err != nil {
		return err
	}
	value.EffectiveConfig = make(map[string][]instanceValue)
	for _, item := range configs.Data.Result {
		number, _ := strconv.ParseFloat(fmt.Sprint(item.Value[1]), 64)
		param := item.Metric["param"]
		value.EffectiveConfig[param] = append(value.EffectiveConfig[param], instanceValue{
			Job: item.Metric["job"], Instance: item.Metric["instance"], Value: number,
		})
	}
	modes, err := queryVM(baseURL, "recap_narrative_provider_mode == 1")
	if err != nil {
		return err
	}
	value.NarrativeModes = make(map[string][]string)
	for _, item := range modes.Data.Result {
		job := item.Metric["job"]
		value.NarrativeModes[job] = append(value.NarrativeModes[job], item.Metric["mode"]+"@"+item.Metric["instance"])
	}
	targets, err := queryVM(baseURL, "sum by (job) (up)")
	if err != nil {
		return err
	}
	value.ScrapeTargets = make(map[string]int)
	for _, item := range targets.Data.Result {
		number, _ := strconv.ParseFloat(fmt.Sprint(item.Value[1]), 64)
		value.ScrapeTargets[item.Metric["job"]] = int(number)
	}
	return nil
}

func commandOutput(name string, args ...string) string {
	value, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(value))
}
