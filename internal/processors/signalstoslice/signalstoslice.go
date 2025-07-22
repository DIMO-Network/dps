package signalstoslice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type sliceProcessor struct {
	logger *service.Logger
}

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec().Description("Signal Json Object to slice.")
	constructor := func(_ *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newSliceProcessor(mgr.Logger()), nil
	}
	err := service.RegisterProcessor("dimo_signal_to_slice", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

func newSliceProcessor(lgr *service.Logger) *sliceProcessor {
	// The logger will already be labelled with the
	// identifier of this component within a config.
	return &sliceProcessor{
		logger: lgr,
	}
}

func (s *sliceProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Extract the message payload as a byte slice.
	payload, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get signal message payload: %w", err)
	}

	var signal vss.Signal
	err = json.Unmarshal(payload, &signal)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal signal: %w", err)
	}

	sig := vss.SignalToSlice(signal)
	msgCpy := msg.Copy()
	msgCpy.SetStructured(sig)

	return []*service.Message{msgCpy}, nil
}

func (p *sliceProcessor) Close(ctx context.Context) error {
	return nil
}
