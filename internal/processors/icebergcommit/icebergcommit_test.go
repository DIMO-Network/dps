package icebergcommit

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestProcess_BuildsIcebergAppendBody(t *testing.T) {
	proc, err := newRequestBuilder("cloudevent", "valid")
	require.NoError(t, err)
	rb := proc.(*requestBuilder)
	defer func() { _ = rb.Close(context.Background()) }()

	msg := service.NewMessage(nil)
	msg.MetaSetMut("dimo_parquet_path", "s3://bucket/cloudevent/valid/year=2025/month=02/day=15/batch-abc.parquet")
	msg.MetaSetMut("dimo_parquet_size", "1024")
	msg.MetaSetMut("dimo_parquet_count", "42")

	out, err := rb.Process(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)

	body, err := out[0].AsBytes()
	require.NoError(t, err)

	var parsed struct {
		Requirements []any `json:"requirements"`
		Updates      []struct {
			Action     string `json:"action"`
			SnapshotID int64  `json:"snapshot-id"`
			DataFiles  []struct {
				FilePath   string `json:"file-path"`
				FileFormat string `json:"file-format"`
				RecordCount int64 `json:"record-count"`
				FileSize   int64 `json:"file-size-in-bytes"`
			} `json:"data-files"`
		} `json:"updates"`
	}
	require.NoError(t, json.Unmarshal(body, &parsed))
	require.Empty(t, parsed.Requirements)
	require.Len(t, parsed.Updates, 1)
	require.Equal(t, "append", parsed.Updates[0].Action)
	require.NotZero(t, parsed.Updates[0].SnapshotID)
	require.Len(t, parsed.Updates[0].DataFiles, 1)
	require.Equal(t, "s3://bucket/cloudevent/valid/year=2025/month=02/day=15/batch-abc.parquet", parsed.Updates[0].DataFiles[0].FilePath)
	require.Equal(t, "PARQUET", parsed.Updates[0].DataFiles[0].FileFormat)
	require.Equal(t, int64(42), parsed.Updates[0].DataFiles[0].RecordCount)
	require.Equal(t, int64(1024), parsed.Updates[0].DataFiles[0].FileSize)
}

func TestProcess_MissingParquetPath(t *testing.T) {
	proc, err := newRequestBuilder("cloudevent", "partial")
	require.NoError(t, err)
	rb := proc.(*requestBuilder)
	defer func() { _ = rb.Close(context.Background()) }()

	msg := service.NewMessage(nil)
	// no dimo_parquet_path

	_, err = rb.Process(context.Background(), msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dimo_parquet_path")
}

// newRequestBuilder is used by tests to construct the processor without going through config parsing.
func newRequestBuilder(_, _ string) (service.Processor, error) {
	return &requestBuilder{}, nil
}
