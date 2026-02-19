// Package cloudeventtoparquet provides a Benthos batch processor that converts a batch of
// CloudEvent messages into Parquet messages (one per subject+source group) plus the original
// messages with metadata. Paths are partitioned by subject and source for Iceberg partition
// pruning on car-based queries. Each Parquet message has dimo_s3_upload_key for aws_s3;
// originals carry dimo_cloudevent_index and (on the first of each group) dimo_parquet_* for
// Iceberg commit and ClickHouse indexing.
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
	Summary("Converts a batch of CloudEvents to Parquet messages (one per subject+source group) with subject/source/day paths for partition pruning, plus originals with metadata for aws_s3 and downstream.").
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

	// Group message indices by (subject, source) for partition pruning.
	groups, err := groupBySubjectSource(msgs)
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
			return nil, fmt.Errorf("encode parquet for subject=%q source=%q: %w", g.subject, g.source, err)
		}

		objectKey := buildObjectKey(p.prefix, g.subject, g.source, now)
		fullPath := fmt.Sprintf("s3://%s/%s", p.bucket, objectKey)
		fileSize := len(parquetBytes)
		recordCount := len(g.indices)

		parquetMsg := service.NewMessage(parquetBytes)
		parquetMsg.MetaSetMut(MetaS3Bucket, p.bucket)
		parquetMsg.MetaSetMut(MetaS3UploadKey, objectKey)
		parquetMsg.MetaSetMut(MetaParquetPath, fullPath)
		parquetMsg.MetaSetMut(MetaParquetSize, strconv.Itoa(fileSize))
		parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(recordCount))
		out = append(out, parquetMsg)

		indexKeyPrefix := fullPath + "#"
		for i, idx := range g.indices {
			msg := msgs[idx]
			msg.MetaSetMut(MetaCloudeventIndex, indexKeyPrefix+strconv.Itoa(i))
			if i == 0 {
				msg.MetaSetMut(MetaParquetPath, fullPath)
				msg.MetaSetMut(MetaParquetSize, strconv.Itoa(fileSize))
				msg.MetaSetMut(MetaParquetCount, strconv.Itoa(recordCount))
			}
		}
	}

	out = append(out, msgs...)
	return []service.MessageBatch{out}, nil
}

type subjectSourceGroup struct {
	subject, source string
	indices         []int
}

func groupBySubjectSource(msgs service.MessageBatch) ([]subjectSourceGroup, error) {
	keyToIndices := make(map[string]*subjectSourceGroup)
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
		subject := sanitizePartitionValue(raw.Subject)
		source := sanitizePartitionValue(raw.Source)
		key := subject + "\x00" + source
		if keyToIndices[key] == nil {
			keyToIndices[key] = &subjectSourceGroup{subject: subject, source: source, indices: nil}
			order = append(order, key)
		}
		keyToIndices[key].indices = append(keyToIndices[key].indices, i)
	}
	groups := make([]subjectSourceGroup, 0, len(order))
	for _, k := range order {
		groups = append(groups, *keyToIndices[k])
	}
	return groups, nil
}

// sanitizePartitionValue makes a value safe for use in object key path segments
// (e.g. subject=.../source=...). Replaces / and \ with _; empty becomes "_".
func sanitizePartitionValue(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	return s
}

func buildObjectKey(prefix, subject, source string, t time.Time) string {
	return fmt.Sprintf("%ssubject=%s/source=%s/year=%d/month=%02d/day=%02d/batch-%s.parquet",
		prefix, subject, source, t.Year(), t.Month(), t.Day(), uuid.New().String())
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
