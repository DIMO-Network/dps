package parquetwriter

import (
	"testing"

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
