package main

import (
	"github.com/conductor-sdk/conductor-go/sdk/model"
	"log/slog"
	"os"
	"time"

	"github.com/conductor-sdk/conductor-go/sdk/client"
	"github.com/conductor-sdk/conductor-go/sdk/settings"
	"github.com/conductor-sdk/conductor-go/sdk/workflow/executor"
	"github.com/robotjoosen/go-config"
)

const (
	WorkflowIdentifier = "greetings"
)

var (
	buildName    = "conductor-sensor-generic"
	buildVersion = "dev"
)

type Settings struct {
	ConductorURL    string       `mapstructure:"CONDUCTOR_URL" json:"conductor_url"`
	ConductorKey    secretString `mapstructure:"CONDUCTOR_KEY" json:"conductor_key"`
	ConductorSecret secretString `mapstructure:"CONDUCTOR_SECRET" json:"conductor_secret"`
}

type secretString string

func (s secretString) LogValue() slog.Value {
	return slog.StringValue("***")
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(
		slog.String("build_name", buildName),
		slog.String("build_version", buildVersion),
	))

	var s Settings
	if _, err := config.Load(&s, map[string]any{
		"CONDUCTOR_URL": "http://application.conductor:8080",
	}); err != nil {
		slog.Error("loading configuration failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	slog.Info("using configuration", slog.Any("settings", s))

	c := client.NewAPIClient(nil, settings.NewHttpSettings(s.ConductorURL))
	e := executor.NewWorkflowExecutor(c)

	if c == nil || e == nil {
		os.Exit(1)
	}

	t := time.NewTicker(time.Second * 10)

	for {
		slog.Info("waiting for trigger")

		select {
		case <-t.C:
			id, err := e.StartWorkflow(
				&model.StartWorkflowRequest{
					Name:    WorkflowIdentifier,
					Version: 1,
					Input: map[string]string{
						"name": "Gopher",
					},
				},
			)
			if err != nil {
				slog.Error("failed to start workflow", slog.String("error", err.Error()))
				os.Exit(1)
			}

			slog.Info("started workflow", "id", id)

			channel, _ := e.MonitorExecution(id)
			run := <-channel
			slog.Info("output of the workflow", "output", run.Output)
		}
	}
}
