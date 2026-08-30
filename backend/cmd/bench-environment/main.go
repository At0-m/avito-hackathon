package main

import (
	"encoding/json"
	"flag"
	"log"
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
}

func main() {
	output := flag.String("output", "bench/results/environment.json", "output JSON path")
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

func commandOutput(name string, args ...string) string {
	value, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(value))
}
