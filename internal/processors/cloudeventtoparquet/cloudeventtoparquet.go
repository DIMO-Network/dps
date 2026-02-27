// Package cloudeventtoparquet provides a Benthos batch processor that converts a batch of
// CloudEvent messages into Parquet messages (one per source group) plus the original
// messages with metadata. Paths are partitioned by source for partition pruning
// on car-based queries. Each Parquet message has dimo_s3_upload_key and dimo_parquet_* for aws_s3 and downstream; originals
// carry dimo_cloudevent_index (parquet_path#row_offset) for S3 path and ClickHouse indexing.
package cloudeventtoparquet

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
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
	Summary("Converts a batch of CloudEvents to Parquet messages (one per source group) with source/day paths for partition pruning, plus originals with metadata for aws_s3 and downstream.").
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

	// Group message indices by source for partition pruning.
	groups, err := groupBySource(msgs)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	out := make(service.MessageBatch, 0, len(groups)+len(msgs))

	for _, g := range groups {
		payloads := make([][]byte, len(g.indices))
		for i, idx := range g.indices {
			b, err := msgs[idx].AsBytes()
			if err != nil {
				return nil, fmt.Errorf("message %d: get bytes: %w", idx, err)
			}
			payloads[i] = b
		}

		parquetBytes, err := encoders.EncodeToParquet(payloads)
		if err != nil {
			return nil, fmt.Errorf("encode parquet for source=%q: %w", g.source, err)
		}

		objectKey := buildObjectKey(p.prefix, g.source, now)
		fileSize := len(parquetBytes)
		recordCount := len(g.indices)

		parquetMsg := service.NewMessage(parquetBytes)
		parquetMsg.MetaSetMut(MetaS3UploadKey, objectKey)
		parquetMsg.MetaSetMut(MetaParquetPath, objectKey)
		parquetMsg.MetaSetMut(MetaParquetSize, strconv.Itoa(fileSize))
		parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(recordCount))
		out = append(out, parquetMsg)

		indexKeyPrefix := objectKey + "#"
		for i, idx := range g.indices {
			msgs[idx].MetaSetMut(MetaCloudeventIndex, indexKeyPrefix+strconv.Itoa(i))
		}
	}

	out = append(out, msgs...)
	return []service.MessageBatch{out}, nil
}

type sourceGroup struct {
	source  string
	indices []int
}

func groupBySource(msgs service.MessageBatch) ([]sourceGroup, error) {
	keyToIndices := make(map[string]*sourceGroup)
	var order []string
	for i, msg := range msgs {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("message %d: get bytes: %w", i, err)
		}
		var raw cloudevent.RawEvent
		if err := json.Unmarshal(b, &raw); err != nil {
			return nil, fmt.Errorf("message %d: unmarshal cloudevent: %w", i, err)
		}
		source := sanitizePartitionValue(raw.Source)
		if keyToIndices[source] == nil {
			keyToIndices[source] = &sourceGroup{source: source, indices: nil}
			order = append(order, source)
		}
		keyToIndices[source].indices = append(keyToIndices[source].indices, i)
	}
	groups := make([]sourceGroup, 0, len(order))
	for _, k := range order {
		groups = append(groups, *keyToIndices[k])
	}
	return groups, nil
}

// sanitizePartitionValue makes a value safe for use in object key path segments.
// Replaces / and \ with _; empty becomes "_".
func sanitizePartitionValue(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	return s
}

func buildObjectKey(prefix, source string, t time.Time) string {
	return fmt.Sprintf("%s%d/%02d/%02d/%s/batch-%s.parquet",
		prefix, t.Year(), int(t.Month()), t.Day(), source, uuid.New().String())
}

