package timed

import (
	"log/slog"
	"os"
	"time"

	"github.com/conductor-sdk/conductor-go/sdk/model"
	"github.com/conductor-sdk/conductor-go/sdk/workflow/executor"
	"github.com/robotjoosen/go-conductor-sensor-generic/internal/domain"
)

func Initialise(duration time.Duration, e *executor.WorkflowExecutor) {
	t := time.NewTicker(duration)
	defer t.Stop()

	go func() {
		for {
			slog.Info("waiting for trigger")

			select {
			case <-t.C:
				id, err := e.StartWorkflow(
					&model.StartWorkflowRequest{
						Name:    domain.GreetingWorkflowIdentifier,
						Version: 4,
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
	}()
}
