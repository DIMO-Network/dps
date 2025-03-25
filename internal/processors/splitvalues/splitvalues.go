package splitvalues

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/pkg/clickhouse"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	cloudEventIndexValueKey = "dimo_cloudevent_index_value"
	cloudeventIndexKey      = "dimo_cloudevent_index"
	oldIndexSize            = 9
)

var configSpec = service.NewConfigSpec().
	Summary("Pulls index values off a cloudevent message and returns seperate messagaes for each field")

func init() {
	err := service.RegisterBatchProcessor("dimo_split_values", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return processor{}, nil
}

type processor struct{}

// Close to fulfill the service.Processor interface.
func (processor) Close(context.Context) error { return nil }

// ProcessBatch to fulfill the service.Processor sets an error on each message.
func (p processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retMsgs []*service.Message
	for _, msg := range msgs {
		values, ok := msg.MetaGetMut(cloudEventIndexValueKey)
		if !ok {
			return nil, errors.New("no index values found")
		}
		// Convert values to string
		valuesStr, ok := values.(string)
		if !ok {
			return nil, fmt.Errorf("index values is not a string instead is %T", values)
		}
		rawValues := []byte(valuesStr)

		hdrs := []*cloudevent.CloudEventHeader{}
		err := json.Unmarshal(rawValues, &hdrs)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal cloud event headers: %w", err)
		}

		indexKey, ok := msg.MetaGet(cloudeventIndexKey)
		if !ok {
			return nil, errors.New("no index key found")
		}
		for _, hdr := range hdrs {
			vals := clickhouse.CloudEventToSliceWithKey(hdr, indexKey)
			newMsg := msg.Copy()
			newMsg.SetStructured(vals)
			retMsgs = append(retMsgs, newMsg)
		}
	}
	return []service.MessageBatch{retMsgs}, nil
}
