package cloudeventtoparquet

import (
	"context"
	"strings"
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
	assert.Contains(t, key, "subject=sub/")
	assert.Contains(t, key, "source=s/")
	assert.Contains(t, key, "year=")
	assert.Contains(t, key, "batch-")
	assert.Contains(t, key, ".parquet")
	bucket, _ := parquetMsg.MetaGet(MetaS3Bucket)
	assert.Equal(t, "my-bucket", bucket)
}

func TestProcessBatch_GroupBySubjectSource(t *testing.T) {
	mgr := service.MockResources()
	proc := &processor{logger: mgr.Logger(), bucket: "b", prefix: "p/"}
	defer func() { _ = proc.Close(context.Background()) }()

	msg1 := service.NewMessage([]byte(`{"id":"a","source":"src1","producer":"p","specversion":"1.0","subject":"car1","time":"2025-01-01T00:00:00Z","type":"t"}`))
	msg2 := service.NewMessage([]byte(`{"id":"b","source":"src2","producer":"p","specversion":"1.0","subject":"car2","time":"2025-01-01T00:00:00Z","type":"t"}`))
	msg3 := service.NewMessage([]byte(`{"id":"c","source":"src1","producer":"p","specversion":"1.0","subject":"car1","time":"2025-01-01T00:00:00Z","type":"t"}`))
	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg1, msg2, msg3})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	// 2 groups (car1+src1, car2+src2) -> 2 parquet msgs + 3 originals
	require.Len(t, batches[0], 5)
	keys := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		k, _ := batches[0][i].MetaGet(MetaS3UploadKey)
		keys = append(keys, k)
	}
	assert.Contains(t, keys[0], "subject=car1/source=src1/")
	assert.Contains(t, keys[1], "subject=car2/source=src2/")
	// Originals: only first of each group has dimo_parquet_path; all have dimo_cloudevent_index
	path1, _ := batches[0][2].MetaGet(MetaParquetPath)
	path2, _ := batches[0][3].MetaGet(MetaParquetPath)
	path3, _ := batches[0][4].MetaGet(MetaParquetPath)
	assert.Contains(t, path1, "subject=car1/")
	assert.Contains(t, path2, "subject=car2/")
	assert.Empty(t, path3) // second in group does not get dimo_parquet_path
	idx1, _ := batches[0][2].MetaGet(MetaCloudeventIndex)
	idx3, _ := batches[0][4].MetaGet(MetaCloudeventIndex)
	assert.Contains(t, idx1, "subject=car1/")
	assert.Contains(t, idx3, "subject=car1/")
	// Same file: index is path#offset, so prefix (path) should match for same group
	assert.Equal(t, idx1[:strings.Index(idx1, "#")], idx3[:strings.Index(idx3, "#")])
}

func TestSanitizePartitionValue(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"sub", "sub"},
		{"a/b", "a_b"},
		{"a\\b", "a_b"},
		{"", "_"},
		{"  ", "_"},
		{" /x/ ", "_x_"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := sanitizePartitionValue(tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}
