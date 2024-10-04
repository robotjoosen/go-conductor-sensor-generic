package messagequeue

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/conductor-sdk/conductor-go/sdk/model"
	"github.com/conductor-sdk/conductor-go/sdk/workflow/executor"
	"github.com/google/uuid"
	"github.com/robotjoosen/go-conductor-sensor-generic/internal/domain"
	"github.com/robotjoosen/go-rabbit"
	"github.com/wagslane/go-rabbitmq"
)

type (
	Record struct {
		ID string `json:"id"`
	}

	Message struct {
		CorrelationID string `json:"correlation_id"`
		ActionType    string `json:"action_type"`
		Data          Record `json:"data" `
	}
)

func Initialise(c *rabbitmq.Conn, e *executor.WorkflowExecutor) {
	hostname := "unknown" + uuid.NewString()
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}

	traceabilityRoutingKeys := []string{
		"record.get",
		"record.patch",
	}

	if err := rabbit.RunConsumer(c, "traceability", traceabilityRoutingKeys, "traceability-"+hostname, func(d rabbitmq.Delivery) (action rabbitmq.Action) {
		dLog := slog.With(
			slog.String("message_id", d.MessageId),
			slog.String("correlation_id", d.CorrelationId),
			slog.String("routing_key", d.RoutingKey),
		)

		var msg Message
		err := json.Unmarshal(d.Body, &msg)
		if err != nil {
			dLog.Error("failed to unmarshal message")

			return rabbitmq.NackDiscard
		}

		id, err := e.StartWorkflow(
			&model.StartWorkflowRequest{
				CorrelationId: msg.CorrelationID,
				Name:          domain.GreetingWorkflowIdentifier,
				Input: map[string]string{
					"name": msg.Data.ID,
				},
			},
		)
		if err != nil {
			slog.Error("failed to start workflow", slog.String("error", err.Error()))

			return
		}

		slog.Info("started workflow", "id", id)

		go monitorExecution(id, e) // monitor the execution in a subroutine

		return rabbitmq.Ack
	}, context.Background()); err != nil {
		slog.Error("failed to run consumer", slog.String("error", err.Error()))

		return
	}
}

func monitorExecution(id string, e *executor.WorkflowExecutor) {
	channel, _ := e.MonitorExecution(id)
	run := <-channel
	slog.Info("output of the workflow", "output", run.Output)
}
