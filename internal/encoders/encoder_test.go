package encoders

import (
	"testing"

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
