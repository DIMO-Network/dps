package splitvalues

import (
	"context"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
)

func TestProcessBatch(t *testing.T) {
	// Create a mock message with metadata
	msg := service.NewMessage(nil)
	input := `[["000000000001388245fbCD3ef7361d156e8b16F5538AE36DEdf61Da8000001af", "2024-12-12T16:00:33Z", "MA", "F26421509Efe92861a587482100c6d728aBf1CD0", "!!!!!!!!!!!!!!r/v0/s","00", "0000000000013882325b45949C833986bC98e98a49F3CA5C5c4643B50000000e", "", "000000000001388245fbCD3ef7361d156e8b16F5538AE36DEdf61Da8000001af758787160033MAF26421509Efe92861a587482100c6d728aBf1CD0!!!!!!!!!!!!!!r/v0/s000000000000013882325b45949C833986bC98e98a49F3CA5C5c4643B50000000e"]]`
	msg.MetaSetMut(CloudEventIndexValueKey, input)

	// Create a message batch
	msgs := service.MessageBatch{msg}

	// Create the processor
	p := processor{}

	// Call ProcessBatch
	result, err := p.ProcessBatch(context.Background(), msgs)

	// Verify no error
	assert.NoError(t, err)

	// Verify the output messages
	assert.Len(t, result, 1)
	assert.Len(t, result[0], 1)
}
