package splitvalues

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessBatch(t *testing.T) {
	// Create a fixed time for consistent testing
	fixedTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		indexValue    string
		indexKey      string
		expectedCount int
		expectedError bool
	}{
		{
			name:          "valid legacy format",
			indexValue:    `[["000000000001388245fbCD3ef7361d156e8b16F5538AE36DEdf61Da8000001af", "2024-12-12T16:00:33Z", "MA", "F26421509Efe92861a587482100c6d728aBf1CD0", "!!!!!!!!!!!!!!r/v0/s","00", "0000000000013882325b45949C833986bC98e98a49F3CA5C5c4643B50000000e", "", "000000000001388245fbCD3ef7361d156e8b16F5538AE36DEdf61Da8000001af758787160033MAF26421509Efe92861a587482100c6d728aBf1CD0!!!!!!!!!!!!!!r/v0/s000000000000013882325b45949C833986bC98e98a49F3CA5C5c4643B50000000e"]]`,
			indexKey:      "test_index",
			expectedCount: 1,
		},
		{
			name:          "valid partial legacy format",
			indexValue:    `[["", "2024-12-12T16:00:33Z", "MA", "F26421509Efe92861a587482100c6d728aBf1CD0", "!!!!!!!!!!!!!!r/v0/s","00", "0000000000013882325b45949C833986bC98e98a49F3CA5C5c4643B50000000e", "", "000000000001388245fbCD3ef7361d156e8b16F5538AE36DEdf61Da8000001af758787160033MAF26421509Efe92861a587482100c6d728aBf1CD0!!!!!!!!!!!!!!r/v0/s000000000000013882325b45949C833986bC98e98a49F3CA5C5c4643B50000000e"]]`,
			indexKey:      "test_index",
			expectedCount: 1,
		},
		{
			name:          "missing index values",
			indexValue:    "",
			indexKey:      "test_index",
			expectedError: true,
		},
		{
			name:          "invalid JSON in index values",
			indexValue:    "invalid json",
			indexKey:      "test_index",
			expectedError: true,
		},
		{
			name: "missing index key",
			indexValue: createTestCloudEventHeaders([]cloudevent.CloudEventHeader{
				{
					ID:          "test-id-1",
					Source:      "device/123",
					Producer:    "test-producer",
					SpecVersion: "1.0",
					Subject:     "test-subject",
					Time:        fixedTime,
					Type:        "device.event.v1",
					Extras: map[string]any{
						"test_index": "test_value",
					},
				},
			}),
			expectedError: true,
		},
		{
			name: "valid cloud event header format",
			indexValue: createTestCloudEventHeaders([]cloudevent.CloudEventHeader{
				{
					ID:          "test-id-1",
					Source:      "device/123",
					Producer:    "test-producer",
					SpecVersion: "1.0",
					Subject:     "test-subject",
					Time:        fixedTime,
					Type:        "device.event.v1",
					Extras: map[string]any{
						"test_index": "test_value",
					},
				},
			}),
			indexKey:      "test_index",
			expectedCount: 1,
		},
		{
			name: "multiple cloud event headers",
			indexValue: createTestCloudEventHeaders([]cloudevent.CloudEventHeader{
				{
					ID:          "test-id-1",
					Source:      "device/123",
					Producer:    "test-producer",
					SpecVersion: "1.0",
					Subject:     "test-subject",
					Time:        fixedTime,
					Type:        "device.event.v1",
					Extras: map[string]any{
						"test_index": "value1",
					},
				},
				{
					ID:          "test-id-2",
					Source:      "device/456",
					Producer:    "test-producer",
					SpecVersion: "1.0",
					Subject:     "test-subject",
					Time:        fixedTime,
					Type:        "device.event.v1",
					Extras: map[string]any{
						"test_index": "value2",
					},
				},
			}),
			indexKey:      "test_index",
			expectedCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := service.NewMessage(nil)
			msg.MetaSetMut(cloudEventIndexValueKey, tt.indexValue)
			if tt.indexKey != "" {
				msg.MetaSetMut(cloudeventIndexKey, tt.indexKey)
			}

			msgs := service.MessageBatch{msg}
			p := processor{}

			result, err := p.ProcessBatch(context.Background(), msgs)

			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, result, 1)
			assert.Len(t, result[0], tt.expectedCount)
			for _, newMsg := range result[0] {
				vals, err := newMsg.AsStructured()
				require.NoError(t, err)
				valsSlice, ok := vals.([]any)
				require.True(t, ok)
				require.Len(t, valsSlice, 10)
			}
		})
	}
}

// createTestCloudEventHeaders helper function to create JSON string of CloudEventHeaders
func createTestCloudEventHeaders(headers []cloudevent.CloudEventHeader) string {
	bytes, err := json.Marshal(headers)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}
