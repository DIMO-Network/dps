// Package parquet provides a Benthos batch processor that converts a batch of
// CloudEvent messages into a single Parquet message plus the original messages with metadata.
// Each Parquet message has dimo_s3_upload_key and dimo_parquet_* for aws_s3 and downstream; originals
// carry dimo_cloudevent_index (parquet_path#row_offset) for S3 path and ClickHouse indexing.
package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/DIMO-Network/cloudevent"
	pq "github.com/DIMO-Network/cloudevent/parquet"
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
	return &processor{prefix: prefix, logger: mgr.Logger()}, nil
}

type processor struct {
	prefix string
	logger *service.Logger
}

func (p *processor) Close(context.Context) error { return nil }

func (p *processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msgs) == 0 {
		return []service.MessageBatch{msgs}, nil
	}

	// Unmarshal each message individually, skip bad ones.
	type goodMsg struct {
		event cloudevent.RawEvent
		msg   *service.Message
	}
	var good []goodMsg
	for i, msg := range msgs {
		b, err := msg.AsBytes()
		if err != nil {
			p.logger.Warnf("message %d: get bytes: %v, skipping", i, err)
			continue
		}
		var ev cloudevent.RawEvent
		if err := json.Unmarshal(b, &ev); err != nil {
			p.logger.Warnf("message %d: unmarshal cloudevent: %v, skipping", i, err)
			continue
		}
		good = append(good, goodMsg{event: ev, msg: msg})
	}

	if len(good) == 0 {
		return []service.MessageBatch{}, nil
	}

	events := make([]cloudevent.RawEvent, len(good))
	for i, g := range good {
		events[i] = g.event
	}

	now := time.Now().UTC()
	objectKey := buildObjectKey(p.prefix, now)

	var buf bytes.Buffer
	indexKeyMap, err := pq.Encode(&buf, events, objectKey)
	if err != nil {
		return nil, fmt.Errorf("encode parquet: %w", err)
	}

	parquetBytes := buf.Bytes()
	parquetMsg := service.NewMessage(parquetBytes)
	parquetMsg.MetaSetMut(MetaS3UploadKey, objectKey)
	parquetMsg.MetaSetMut(MetaParquetPath, objectKey)
	parquetMsg.MetaSetMut(MetaParquetSize, strconv.Itoa(len(parquetBytes)))
	parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(len(good)))

	for i, g := range good {
		g.msg.MetaSetMut(MetaCloudeventIndex, indexKeyMap[i])
	}

	out := make(service.MessageBatch, 0, 1+len(good))
	out = append(out, parquetMsg)
	for _, g := range good {
		out = append(out, g.msg)
	}
	return []service.MessageBatch{out}, nil
}

func buildObjectKey(prefix string, t time.Time) string {
	return fmt.Sprintf("%s%d/%02d/%02d/batch-%s.parquet",
		prefix, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}
