package eventToSlice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/model-garage/pkg/occurrences"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type eventProcessor struct {
	logger *service.Logger
}

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec().Description("Event Json Object to slice.")
	constructor := func(_ *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newEventProcessor(mgr.Logger()), nil
	}
	err := service.RegisterProcessor("event_to_slice", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

func newEventProcessor(lgr *service.Logger) *eventProcessor {
	// The logger will already be labelled with the
	// identifier of this component within a config.
	return &eventProcessor{
		logger: lgr,
	}
}

func (s *eventProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Extract the message payload as a byte slice.
	payload, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	var event occurrences.Event
	err = json.Unmarshal(payload, &event)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	sig := occurrences.EventToSlice(event)
	msgCpy := msg.Copy()
	msgCpy.SetStructured(sig)

	return []*service.Message{msgCpy}, nil
}

func (p *eventProcessor) Close(ctx context.Context) error {
	return nil
}
