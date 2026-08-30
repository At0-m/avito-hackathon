package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type faultReport struct {
	Scenario    string        `json:"scenario"`
	Service     string        `json:"service"`
	Action      string        `json:"action"`
	Delay       time.Duration `json:"delay"`
	Outage      time.Duration `json:"outage"`
	StartedAt   time.Time     `json:"started_at"`
	FaultAt     time.Time     `json:"fault_at"`
	RecoveredAt time.Time     `json:"recovered_at"`
	Duration    time.Duration `json:"duration"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
}

func main() {
	service := flag.String("service", "worker", "Docker Compose service to disrupt")
	action := flag.String("action", "stop-start", "fault action: restart, stop-start or kill-start")
	delay := flag.Duration("delay", 2*time.Second, "delay before injecting the fault")
	outage := flag.Duration("outage", 10*time.Second, "service downtime for stop-start and kill-start")
	composeFile := flag.String("compose-file", "docker-compose.yml", "Docker Compose file")
	output := flag.String("output", "", "optional JSON report path")
	flag.Parse()

	if strings.TrimSpace(*service) == "" {
		log.Fatal("service is required")
	}
	if *delay < 0 || *outage < 0 {
		log.Fatal("delay and outage must not be negative")
	}

	report := faultReport{
		Scenario:  *service + "-" + *action,
		Service:   *service,
		Action:    *action,
		Delay:     *delay,
		Outage:    *outage,
		StartedAt: time.Now().UTC(),
	}
	time.Sleep(*delay)
	report.FaultAt = time.Now().UTC()

	ctx, cancel := context.WithTimeout(context.Background(), *delay+*outage+2*time.Minute)
	defer cancel()
	if err := injectFault(ctx, *composeFile, *service, *action, *outage); err != nil {
		report.Error = err.Error()
	} else {
		report.Success = true
	}
	report.RecoveredAt = time.Now().UTC()
	report.Duration = report.RecoveredAt.Sub(report.FaultAt)

	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if *output == "" {
		fmt.Println(string(encoded))
	} else {
		if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(*output, encoded, 0o644); err != nil {
			log.Fatal(err)
		}
	}
	if !report.Success {
		os.Exit(1)
	}
}

func injectFault(ctx context.Context, composeFile, service, action string, outage time.Duration) error {
	switch action {
	case "restart":
		return compose(ctx, composeFile, "restart", service)
	case "stop-start":
		if err := compose(ctx, composeFile, "stop", service); err != nil {
			return err
		}
		if !sleepContext(ctx, outage) {
			return ctx.Err()
		}
		return compose(ctx, composeFile, "start", service)
	case "kill-start":
		if err := compose(ctx, composeFile, "kill", "-s", "SIGKILL", service); err != nil {
			return err
		}
		if !sleepContext(ctx, outage) {
			return ctx.Err()
		}
		return compose(ctx, composeFile, "start", service)
	default:
		return fmt.Errorf("unsupported action %q", action)
	}
}

func compose(ctx context.Context, composeFile string, args ...string) error {
	commandArgs := []string{"compose", "-f", composeFile}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, "docker", commandArgs...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("docker %s: %w", strings.Join(commandArgs, " "), err)
	}
	return nil
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
