package encoders

import (
	"encoding/json"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaFields(t *testing.T) {
	expected := []string{
		"id", "subject", "source", "producer", "time", "type",
		"data_content_type", "data_version", "specversion", "dataschema",
		"signature", "extras", "data",
	}
	require.Equal(t, len(expected), Schema.NumFields())
	for i, name := range expected {
		require.Equal(t, name, Schema.Field(i).Name, "field %d", i)
	}
}

func TestEncodeToParquet_SingleEvent(t *testing.T) {
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data": "{\"k\":\"v\"}"
	}`)
	out, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, out)
}

func TestEncodeToParquet_EmptyFails(t *testing.T) {
	_, err := EncodeToParquet(nil)
	require.Error(t, err)
	_, err = EncodeToParquet([][]byte{})
	require.Error(t, err)
}

// TestRawEvent_DataBase64 ensures RawEvent unmarshals data_base64 and populates Data.
// Encoder uses RawEvent; this confirms base64-encoded event data is accepted in the pipeline.
func TestRawEvent_DataBase64(t *testing.T) {
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
	assert.Equal(t, []byte("binary\x00payload"), []byte(rawEvent.Data))
}
