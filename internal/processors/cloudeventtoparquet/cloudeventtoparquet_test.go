package cloudeventtoparquet

import (
	"context"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

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
