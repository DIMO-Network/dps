package splitvalues

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const CloudEventIndexValueKey = "dimo_cloudevent_index_value"

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
		values, ok := msg.MetaGetMut(CloudEventIndexValueKey)
		if !ok {
			return nil, errors.New("no index values found")
		}

		// Convert values to string
		valuesStr, ok := values.(string)
		if !ok {
			return nil, fmt.Errorf("index values is not a string instead is %T", values)
		}

		// Variable to hold the result
		var valSlice [][]interface{}

		// Parse the JSON string
		err := json.Unmarshal([]byte(valuesStr), &valSlice)
		if err != nil {
			fmt.Println("Error parsing JSON:", err)
			fmt.Printf("index values data: %v\n", values)
			return nil, fmt.Errorf("index values is not a slice of slices instead is %T", values)
		}

		for _, vals := range valSlice {
			newMsg := msg.Copy()
			newMsg.SetStructured(vals)
			retMsgs = append(retMsgs, newMsg)
		}
	}
	return []service.MessageBatch{retMsgs}, nil
}
