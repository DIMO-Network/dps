package parquetwriter

import (
	"encoding/json"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBucket(t *testing.T) {
	tests := []struct {
		name      string
		warehouse string
		expected  string
		wantErr   bool
	}{
		{name: "standard", warehouse: "s3://my-bucket/warehouse/", expected: "my-bucket"},
		{name: "no trailing slash", warehouse: "s3://my-bucket", expected: "my-bucket"},
		{name: "with path", warehouse: "s3://dimo-iceberg-dev/warehouse/data/", expected: "dimo-iceberg-dev"},
		{name: "invalid prefix", warehouse: "gs://bucket/path/", wantErr: true},
		{name: "empty", warehouse: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBucket(tt.warehouse)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestParquetSchemaFields(t *testing.T) {
	// Verify the schema has all expected fields in the correct order.
	expectedFields := []string{
		"id", "subject", "source", "producer", "time", "type",
		"data_content_type", "data_version", "specversion", "dataschema",
		"signature", "extras", "data",
	}

	require.Equal(t, len(expectedFields), parquetSchema.NumFields())
	for i, name := range expectedFields {
		assert.Equal(t, name, parquetSchema.Field(i).Name, "field %d", i)
	}
}

// TestParseMessages_DataBase64 ensures that when a CloudEvent is received with
// only data_base64 (no "data" field), RawEvent unmarshals and Data is the decoded payload.
// This confirms the pipeline accepts base64-encoded event data.
func TestParseMessages_DataBase64(t *testing.T) {
	// CE JSON with only data_base64. base64("binary\x00payload") = YmluYXJ5AHBheWxvYWQ=
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data_base64": "YmluYXJ5AHBheWxvYWQ="
	}`)
	var rawEvent cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(payload, &rawEvent))
	assert.Equal(t, []byte("binary\x00payload"), []byte(rawEvent.Data), "Data should be decoded payload from data_base64")
}
