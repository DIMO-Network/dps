package cloudeventtoparquet

import (
	"context"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
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
		{name: "with path", warehouse: "s3://dimo-storage-dev/warehouse/data/", expected: "dimo-storage-dev"},
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

func TestProcessBatch_MetadataAndBucket(t *testing.T) {
	mgr := service.MockResources()
	proc := &processor{logger: mgr.Logger(), bucket: "my-bucket", prefix: "cloudevent/valid/"}
	defer func() { _ = proc.Close(context.Background()) }()

	msg := service.NewMessage([]byte(`{"id":"x","source":"s","producer":"p","specversion":"1.0","subject":"sub","time":"2025-01-01T00:00:00Z","type":"t"}`))
	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2) // 1 parquet + 1 original
	parquetMsg := batches[0][0]
	key, _ := parquetMsg.MetaGet(MetaS3UploadKey)
	require.NotEmpty(t, key)
	bucket, _ := parquetMsg.MetaGet(MetaS3Bucket)
	require.Equal(t, "my-bucket", bucket)
}
