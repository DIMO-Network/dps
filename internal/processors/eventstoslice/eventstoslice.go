package eventstoslice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type eventSliceProcessor struct {
	logger *service.Logger
}

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec().Description("Event Json Object to slice.")
	constructor := func(_ *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newEventSliceProcessor(mgr.Logger()), nil
	}
	err := service.RegisterProcessor("dimo_event_to_slice", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

func newEventSliceProcessor(lgr *service.Logger) *eventSliceProcessor {
	// The logger will already be labelled with the
	// identifier of this component within a config.
	return &eventSliceProcessor{
		logger: lgr,
	}
}

func (s *eventSliceProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Extract the message payload as a byte slice.
	payload, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get event message payload: %w", err)
	}

	var event []vss.Event
	err = json.Unmarshal(payload, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}
	msgs := make([]*service.Message, 0, len(event))
	for _, e := range event {
		sig := vss.EventToSlice(e)
		msgCpy := msg.Copy()
		msgCpy.SetStructured(sig)
		msgs = append(msgs, msgCpy)
	}
	return msgs, nil
}

func (p *eventSliceProcessor) Close(ctx context.Context) error {
	return nil
}
