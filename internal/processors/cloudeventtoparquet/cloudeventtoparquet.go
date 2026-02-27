// Package cloudeventtoparquet provides a Benthos batch processor that converts a batch of
// CloudEvent messages into a single Parquet message plus the original messages with metadata.
// Each Parquet message has dimo_s3_upload_key and dimo_parquet_* for aws_s3 and downstream; originals
// carry dimo_cloudevent_index (parquet_path#row_offset) for S3 path and ClickHouse indexing.
package cloudeventtoparquet

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/DIMO-Network/dps/internal/encoders"
	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// MetaS3UploadKey is the object key for the Parquet file (path within bucket).
	// aws_s3 output uses path: ${!metadata("dimo_s3_upload_key")}; bucket comes from env (e.g. PARQUET_BUCKET).
	MetaS3UploadKey = "dimo_s3_upload_key"
	// MetaParquetPath is the object key (path) for downstream ClickHouse.
	MetaParquetPath  = "dimo_parquet_path"
	MetaParquetSize  = "dimo_parquet_size"
	MetaParquetCount = "dimo_parquet_count"
	// MetaCloudeventIndex is the index_key for ClickHouse (parquet_path#row_offset).
	MetaCloudeventIndex = "dimo_cloudevent_index"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts a batch of CloudEvents to a single Parquet message with day-partitioned path, plus originals with metadata for aws_s3 and downstream.").
	Field(service.NewStringField("prefix").Description("Path prefix for object key (e.g. cloudevent/valid/)."))

func init() {
	err := service.RegisterBatchProcessor("dimo_cloudevent_to_parquet", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	prefix, err := conf.FieldString("prefix")
	if err != nil {
		return nil, fmt.Errorf("prefix: %w", err)
	}
	return &processor{prefix: prefix}, nil
}

type processor struct {
	prefix string
}

func (p *processor) Close(context.Context) error { return nil }

func (p *processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msgs) == 0 {
		return []service.MessageBatch{msgs}, nil
	}

	payloads := make([][]byte, len(msgs))
	for i, msg := range msgs {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("message %d: get bytes: %w", i, err)
		}
		payloads[i] = b
	}

	parquetBytes, err := encoders.EncodeToParquet(payloads)
	if err != nil {
		return nil, fmt.Errorf("encode parquet: %w", err)
	}

	now := time.Now().UTC()
	objectKey := buildObjectKey(p.prefix, now)

	parquetMsg := service.NewMessage(parquetBytes)
	parquetMsg.MetaSetMut(MetaS3UploadKey, objectKey)
	parquetMsg.MetaSetMut(MetaParquetPath, objectKey)
	parquetMsg.MetaSetMut(MetaParquetSize, strconv.Itoa(len(parquetBytes)))
	parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(len(msgs)))

	indexKeyPrefix := objectKey + "#"
	for i := range msgs {
		msgs[i].MetaSetMut(MetaCloudeventIndex, indexKeyPrefix+strconv.Itoa(i))
	}

	out := make(service.MessageBatch, 0, 1+len(msgs))
	out = append(out, parquetMsg)
	out = append(out, msgs...)
	return []service.MessageBatch{out}, nil
}

func buildObjectKey(prefix string, t time.Time) string {
	return fmt.Sprintf("%s%d/%02d/%02d/batch-%s.parquet",
		prefix, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}

