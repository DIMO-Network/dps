// Package cloudeventtoparquet provides a Benthos batch processor that converts a batch of
// CloudEvent messages into one Parquet message (body = Parquet bytes) plus the original
// messages with metadata. The Parquet message has dimo_s3_upload_key and related metadata
// so the standard aws_s3 output can upload it; the originals carry dimo_cloudevent_index
// and dimo_parquet_* for downstream Iceberg commit and ClickHouse indexing.
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
	// aws_s3 output uses path: ${!metadata("dimo_s3_upload_key")}.
	MetaS3UploadKey = "dimo_s3_upload_key"
	// MetaS3Bucket is the bucket name for aws_s3 output (bucket: ${!metadata("dimo_s3_bucket")}).
	MetaS3Bucket = "dimo_s3_bucket"
	// MetaParquetPath is the full s3://bucket/key for downstream Iceberg/ClickHouse.
	MetaParquetPath  = "dimo_parquet_path"
	MetaParquetSize  = "dimo_parquet_size"
	MetaParquetCount = "dimo_parquet_count"
	// MetaCloudeventIndex is the index_key for ClickHouse (parquet_path#row_offset).
	MetaCloudeventIndex = "dimo_cloudevent_index"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts a batch of CloudEvents to one Parquet message + originals with metadata for aws_s3 and downstream.").
	Field(service.NewStringField("warehouse").Description("Base path (e.g. s3://bucket/warehouse/).")).
	Field(service.NewStringField("prefix").Description("Path prefix within warehouse (e.g. cloudevent/valid/)."))

func init() {
	err := service.RegisterBatchProcessor("dimo_cloudevent_to_parquet", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	warehouse, err := conf.FieldString("warehouse")
	if err != nil {
		return nil, fmt.Errorf("warehouse: %w", err)
	}
	prefix, err := conf.FieldString("prefix")
	if err != nil {
		return nil, fmt.Errorf("prefix: %w", err)
	}
	bucket, err := parseBucket(warehouse)
	if err != nil {
		return nil, fmt.Errorf("warehouse: %w", err)
	}
	return &processor{
		logger: mgr.Logger(),
		bucket: bucket,
		prefix: prefix,
	}, nil
}

type processor struct {
	logger *service.Logger
	bucket string
	prefix string
}

func (p *processor) Close(context.Context) error { return nil }

func (p *processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msgs) == 0 {
		return []service.MessageBatch{msgs}, nil
	}

	payloads := make([][]byte, 0, len(msgs))
	for i, msg := range msgs {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("message %d: get bytes: %w", i, err)
		}
		payloads = append(payloads, b)
	}

	parquetBytes, err := encoders.EncodeToParquet(payloads)
	if err != nil {
		return nil, fmt.Errorf("encode parquet: %w", err)
	}

	now := time.Now().UTC()
	objectKey := fmt.Sprintf("%syear=%d/month=%02d/day=%02d/batch-%s.parquet",
		p.prefix, now.Year(), now.Month(), now.Day(), uuid.New().String())
	fullPath := fmt.Sprintf("s3://%s/%s", p.bucket, objectKey)
	fileSize := len(parquetBytes)
	recordCount := len(msgs)

	// One message: body = parquet bytes, metadata for aws_s3 (bucket, path) and downstream (path/size/count).
	parquetMsg := service.NewMessage(parquetBytes)
	parquetMsg.MetaSetMut(MetaS3Bucket, p.bucket)
	parquetMsg.MetaSetMut(MetaS3UploadKey, objectKey)
	parquetMsg.MetaSetMut(MetaParquetPath, fullPath)
	parquetMsg.MetaSetMut(MetaParquetSize, strconv.Itoa(fileSize))
	parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(recordCount))

	// Originals: dimo_cloudevent_index = fullPath#row_offset; first message also gets dimo_parquet_*.
	indexKeyPrefix := fullPath + "#"
	for i, msg := range msgs {
		msg.MetaSetMut(MetaCloudeventIndex, indexKeyPrefix+strconv.Itoa(i))
	}
	msgs[0].MetaSetMut(MetaParquetPath, fullPath)
	msgs[0].MetaSetMut(MetaParquetSize, strconv.Itoa(fileSize))
	msgs[0].MetaSetMut(MetaParquetCount, strconv.Itoa(recordCount))

	// Return single batch: [parquet message, ...originals]. Downstream filters: S3 branch keeps only parquet msg, rest keep originals.
	out := make(service.MessageBatch, 0, 1+len(msgs))
	out = append(out, parquetMsg)
	out = append(out, msgs...)
	return []service.MessageBatch{out}, nil
}

func parseBucket(warehouse string) (string, error) {
	if len(warehouse) < 6 || warehouse[:5] != "s3://" {
		return "", fmt.Errorf("warehouse must start with s3://: %s", warehouse)
	}
	rest := warehouse[5:]
	for i, c := range rest {
		if c == '/' {
			return rest[:i], nil
		}
	}
	return rest, nil
}
