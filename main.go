package main

import (
	"github.com/robotjoosen/go-conductor-sensor-generic/internal/timed"
	"log/slog"
	"os"
	"time"

	"github.com/conductor-sdk/conductor-go/sdk/client"
	"github.com/conductor-sdk/conductor-go/sdk/settings"
	"github.com/conductor-sdk/conductor-go/sdk/workflow/executor"
	"github.com/robotjoosen/go-conductor-sensor-generic/internal/messagequeue"
	"github.com/robotjoosen/go-config"
	"github.com/robotjoosen/go-rabbit"
)

var (
	buildName    = "conductor-sensor-generic"
	buildVersion = "dev"
)

type Settings struct {
	ConductorURL    string       `mapstructure:"CONDUCTOR_URL" json:"conductor_url"`
	ConductorKey    secretString `mapstructure:"CONDUCTOR_KEY" json:"conductor_key"`
	ConductorSecret secretString `mapstructure:"CONDUCTOR_SECRET" json:"conductor_secret"`
	RabbitMQAddress secretString `mapstructure:"MQ_ADDRESS"`
}

type secretString string

func (s secretString) String() string {
	return string(s)
}

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
		slog.Error("conductor client or executor not set")
		os.Exit(1)
	}

	// trigger workflows every minute
	timed.Initialise(time.Minute, e)

	// trigger workflows based on MQ
	if con, err := rabbit.NewConnection(s.RabbitMQAddress.String()); err == nil {
		messagequeue.Initialise(con, e)
	}

	<-make(chan bool)
}
