// Package parquetwriter provides a Benthos output plugin that batches CloudEvent
// messages into Parquet files and writes them to S3-compatible object storage.
//
// Each batch becomes a single Parquet file. The plugin sets metadata on each message
// so downstream outputs can publish Iceberg commit messages (via kafka_franz) and
// insert ClickHouse index rows referencing the Parquet location.
package parquetwriter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// cloudeventIndexKey is the metadata key that dimo_split_values reads to get
	// the S3/object key for the ClickHouse cloud_event index_key column.
	cloudeventIndexKey = "dimo_cloudevent_index"
)

// parquetSchema defines the Arrow schema for CloudEvent Parquet files.
// This includes full header fields (for self-contained analytics) plus the data payload.
var parquetSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "subject", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "source", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "producer", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
	{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "data_content_type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "data_version", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "specversion", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "dataschema", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "signature", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "extras", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "data", Type: arrow.BinaryTypes.Binary, Nullable: true},
}, nil)

var configSpec = service.NewConfigSpec().
	Summary("Writes CloudEvent batches as Parquet files to S3-compatible object storage.").
	Description(`
Each batch becomes a single Parquet file under the configured warehouse and prefix.
Sets ` + "`dimo_cloudevent_index`" + ` metadata on each message (parquet_path#row_offset) for downstream indexing.
Sets ` + "`dimo_parquet_path`" + `, ` + "`dimo_parquet_size`" + `, and ` + "`dimo_parquet_count`" + ` metadata on the first message
of each batch so downstream outputs (e.g. kafka_franz) can publish Iceberg commit messages.
Batching is configured via the pipeline ` + "`batching`" + ` block; this output uses maxInFlight=1 for sequential writes.`).
	Field(service.NewStringField("warehouse").Description("Base path for Parquet files (e.g. s3://bucket/warehouse/).")).
	Field(service.NewStringField("prefix").Description("Path prefix within the warehouse (e.g. cloudevent/valid/).")).
	Field(service.NewStringField("storage_endpoint").Description("S3-compatible endpoint URL.")).
	Field(service.NewObjectField("credentials",
		service.NewStringField("access_key").Description("Access key for object storage."),
		service.NewStringField("secret_key").Description("Secret key for object storage."),
	).Description("Object storage credentials."))

func init() {
	err := service.RegisterBatchOutput("dimo_parquet_writer", configSpec, newParquetWriterFromConfig)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newParquetWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	w := &writer{
		logger: mgr.Logger(),
		alloc:  memory.DefaultAllocator,
	}

	if err := parseOutputConfig(conf, w); err != nil {
		return nil, service.BatchPolicy{}, 0, err
	}

	// Empty BatchPolicy: batching is configured in the pipeline batching block.
	// maxInFlight=1: one Parquet file write at a time per replica for ordering.
	return w, service.BatchPolicy{}, 1, nil
}

func parseOutputConfig(conf *service.ParsedConfig, w *writer) error {
	var err error
	if w.warehouse, err = conf.FieldString("warehouse"); err != nil {
		return fmt.Errorf("warehouse: %w", err)
	}
	if w.prefix, err = conf.FieldString("prefix"); err != nil {
		return fmt.Errorf("prefix: %w", err)
	}
	if w.storageEndpoint, err = conf.FieldString("storage_endpoint"); err != nil {
		return fmt.Errorf("storage_endpoint: %w", err)
	}
	if w.accessKey, err = conf.FieldString("credentials", "access_key"); err != nil {
		return fmt.Errorf("credentials.access_key: %w", err)
	}
	if w.secretKey, err = conf.FieldString("credentials", "secret_key"); err != nil {
		return fmt.Errorf("credentials.secret_key: %w", err)
	}
	return nil
}

//------------------------------------------------------------------------------

type writer struct {
	logger          *service.Logger
	alloc           memory.Allocator
	warehouse       string
	prefix          string
	storageEndpoint string
	accessKey       string
	secretKey       string
	s3Client        *s3.Client
}

// Connect initializes the S3 client. Idempotent: safe to call multiple times.
func (w *writer) Connect(ctx context.Context) error {
	if w.s3Client != nil {
		return nil
	}
	w.s3Client = s3.New(s3.Options{
		BaseEndpoint: &w.storageEndpoint,
		Credentials:  credentials.NewStaticCredentialsProvider(w.accessKey, w.secretKey, ""),
		Region:       "us-east-2",
	})
	w.logger.Info("Parquet writer connected to object storage")
	return nil
}

// WriteBatch encodes a batch of CloudEvent messages as a Parquet file, writes it
// to object storage, and sets metadata on each message with the Parquet file reference.
func (w *writer) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	if len(msgs) == 0 {
		return nil
	}
	if w.s3Client == nil {
		if err := w.Connect(ctx); err != nil {
			return err
		}
	}

	// Parse all messages into CloudEvent structs.
	events, rawDataSlices, err := w.parseMessages(msgs)
	if err != nil {
		return fmt.Errorf("parse messages: %w", err)
	}

	// Build the Arrow record batch.
	record, err := w.buildRecord(events, rawDataSlices)
	if err != nil {
		return fmt.Errorf("build arrow record: %w", err)
	}
	defer record.Release()

	// Encode as Parquet into a buffer.
	var buf bytes.Buffer
	pqWriter, err := pqarrow.NewFileWriter(parquetSchema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}
	if err := pqWriter.Write(record); err != nil {
		return fmt.Errorf("write parquet record: %w", err)
	}
	if err := pqWriter.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}

	// Generate the object key with date partitioning.
	now := time.Now().UTC()
	objectKey := fmt.Sprintf("%syear=%d/month=%02d/day=%02d/batch-%s.parquet",
		w.prefix, now.Year(), now.Month(), now.Day(), uuid.New().String())

	// Extract bucket from warehouse URL (s3://bucket/path/ -> bucket).
	bucket, err := parseBucket(w.warehouse)
	if err != nil {
		return fmt.Errorf("parse warehouse bucket: %w", err)
	}

	// Upload Parquet file to object storage.
	_, err = w.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("upload parquet to object storage: %w", err)
	}

	fileSize := buf.Len()
	recordCount := len(events)
	fullPath := fmt.Sprintf("s3://%s/%s", bucket, objectKey)
	w.logger.Debugf("Wrote Parquet file %s (%d events, %d bytes)", fullPath, recordCount, fileSize)

	// Set the index_key metadata on each message so dimo_split_values can pick it up.
	// Format: parquet_path#row_offset
	for i, msg := range msgs {
		indexKey := objectKey + "#" + strconv.Itoa(i)
		msg.MetaSetMut(cloudeventIndexKey, indexKey)
	}

	// Set parquet commit metadata on the first message only.
	// Downstream kafka_franz output uses these to publish one Iceberg commit message per file.
	msgs[0].MetaSetMut("dimo_parquet_path", fullPath)
	msgs[0].MetaSetMut("dimo_parquet_size", strconv.Itoa(fileSize))
	msgs[0].MetaSetMut("dimo_parquet_count", strconv.Itoa(recordCount))

	return nil
}

// Close shuts down the writer and releases the S3 client.
func (w *writer) Close(ctx context.Context) error {
	w.s3Client = nil
	w.logger.Info("Parquet writer closed")
	return nil
}

// parseMessages extracts CloudEvent headers and raw data from each Benthos message.
func (w *writer) parseMessages(msgs service.MessageBatch) ([]*cloudevent.CloudEventHeader, [][]byte, error) {
	headers := make([]*cloudevent.CloudEventHeader, 0, len(msgs))
	rawData := make([][]byte, 0, len(msgs))

	for i, msg := range msgs {
		payload, err := msg.AsBytes()
		if err != nil {
			return nil, nil, fmt.Errorf("message %d: get bytes: %w", i, err)
		}

		var rawEvent cloudevent.RawEvent
		if err := json.Unmarshal(payload, &rawEvent); err != nil {
			return nil, nil, fmt.Errorf("message %d: unmarshal cloudevent: %w", i, err)
		}

		headers = append(headers, &rawEvent.CloudEventHeader)
		rawData = append(rawData, rawEvent.Data)
	}

	return headers, rawData, nil
}

//------------------------------------------------------------------------------

// buildRecord creates an Arrow record batch from parsed CloudEvent data.
func (w *writer) buildRecord(headers []*cloudevent.CloudEventHeader, rawData [][]byte) (arrow.Record, error) {
	n := len(headers)

	idBuilder := array.NewStringBuilder(w.alloc)
	defer idBuilder.Release()
	subjectBuilder := array.NewStringBuilder(w.alloc)
	defer subjectBuilder.Release()
	sourceBuilder := array.NewStringBuilder(w.alloc)
	defer sourceBuilder.Release()
	producerBuilder := array.NewStringBuilder(w.alloc)
	defer producerBuilder.Release()
	timeBuilder := array.NewTimestampBuilder(w.alloc, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	defer timeBuilder.Release()
	typeBuilder := array.NewStringBuilder(w.alloc)
	defer typeBuilder.Release()
	dataContentTypeBuilder := array.NewStringBuilder(w.alloc)
	defer dataContentTypeBuilder.Release()
	dataVersionBuilder := array.NewStringBuilder(w.alloc)
	defer dataVersionBuilder.Release()
	specVersionBuilder := array.NewStringBuilder(w.alloc)
	defer specVersionBuilder.Release()
	dataSchemaBuilder := array.NewStringBuilder(w.alloc)
	defer dataSchemaBuilder.Release()
	signatureBuilder := array.NewStringBuilder(w.alloc)
	defer signatureBuilder.Release()
	extrasBuilder := array.NewStringBuilder(w.alloc)
	defer extrasBuilder.Release()
	dataBuilder := array.NewBinaryBuilder(w.alloc, arrow.BinaryTypes.Binary)
	defer dataBuilder.Release()

	for i := 0; i < n; i++ {
		hdr := headers[i]
		idBuilder.Append(hdr.ID)
		subjectBuilder.Append(hdr.Subject)
		sourceBuilder.Append(hdr.Source)
		producerBuilder.Append(hdr.Producer)
		timeBuilder.Append(arrow.Timestamp(hdr.Time.UnixMicro()))
		typeBuilder.Append(hdr.Type)
		dataContentTypeBuilder.Append(hdr.DataContentType)
		dataVersionBuilder.Append(hdr.DataVersion)
		specVersionBuilder.Append(hdr.SpecVersion)
		dataSchemaBuilder.Append(hdr.DataSchema)
		signatureBuilder.Append(hdr.Signature)

		// Serialize extras (tags + arbitrary fields) as JSON.
		extras := make(map[string]any)
		for k, v := range hdr.Extras {
			extras[k] = v
		}
		if len(hdr.Tags) > 0 {
			extras["tags"] = hdr.Tags
		}
		extrasJSON, _ := json.Marshal(extras)
		extrasBuilder.Append(string(extrasJSON))

		dataBuilder.Append(rawData[i])
	}

	cols := []arrow.Array{
		idBuilder.NewArray(),
		subjectBuilder.NewArray(),
		sourceBuilder.NewArray(),
		producerBuilder.NewArray(),
		timeBuilder.NewArray(),
		typeBuilder.NewArray(),
		dataContentTypeBuilder.NewArray(),
		dataVersionBuilder.NewArray(),
		specVersionBuilder.NewArray(),
		dataSchemaBuilder.NewArray(),
		signatureBuilder.NewArray(),
		extrasBuilder.NewArray(),
		dataBuilder.NewArray(),
	}

	// Release each column array after the record takes ownership.
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()

	return array.NewRecord(parquetSchema, cols, int64(n)), nil
}

// parseBucket extracts the bucket name from an s3:// URL.
func parseBucket(warehouse string) (string, error) {
	// Expected format: s3://bucket/path/ or s3://bucket/
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
