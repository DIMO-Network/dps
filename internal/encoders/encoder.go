// Package encoders encodes CloudEvent JSON payloads into a single Parquet buffer.
// Used by the dimo_cloudevent_to_parquet batch processor so the standard aws_s3 output can upload.
package encoders

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// Schema defines the Arrow schema for CloudEvent Parquet files.
var Schema = arrow.NewSchema([]arrow.Field{
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

// EncodeToParquet parses each JSON payload as a CloudEvent (RawEvent), builds an Arrow record,
// and encodes it to Parquet. Returns the Parquet bytes.
func EncodeToParquet(payloads [][]byte) ([]byte, error) {
	if len(payloads) == 0 {
		return nil, fmt.Errorf("no payloads")
	}
	headers, rawData, err := parsePayloads(payloads)
	if err != nil {
		return nil, err
	}
	record, err := buildRecord(headers, rawData)
	if err != nil {
		return nil, err
	}
	defer record.Release()
	var buf bytes.Buffer
	pqWriter, err := pqarrow.NewFileWriter(Schema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}
	if err := pqWriter.Write(record); err != nil {
		return nil, fmt.Errorf("write parquet record: %w", err)
	}
	if err := pqWriter.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

func parsePayloads(payloads [][]byte) ([]*cloudevent.CloudEventHeader, [][]byte, error) {
	headers := make([]*cloudevent.CloudEventHeader, 0, len(payloads))
	rawData := make([][]byte, 0, len(payloads))
	for i, payload := range payloads {
		var rawEvent cloudevent.RawEvent
		if err := json.Unmarshal(payload, &rawEvent); err != nil {
			return nil, nil, fmt.Errorf("payload %d: unmarshal cloudevent: %w", i, err)
		}
		headers = append(headers, &rawEvent.CloudEventHeader)
		rawData = append(rawData, rawEvent.Data)
	}
	return headers, rawData, nil
}

func buildRecord(headers []*cloudevent.CloudEventHeader, rawData [][]byte) (arrow.Record, error) {
	alloc := memory.DefaultAllocator
	n := len(headers)

	idBuilder := array.NewStringBuilder(alloc)
	defer idBuilder.Release()
	subjectBuilder := array.NewStringBuilder(alloc)
	defer subjectBuilder.Release()
	sourceBuilder := array.NewStringBuilder(alloc)
	defer sourceBuilder.Release()
	producerBuilder := array.NewStringBuilder(alloc)
	defer producerBuilder.Release()
	timeBuilder := array.NewTimestampBuilder(alloc, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	defer timeBuilder.Release()
	typeBuilder := array.NewStringBuilder(alloc)
	defer typeBuilder.Release()
	dataContentTypeBuilder := array.NewStringBuilder(alloc)
	defer dataContentTypeBuilder.Release()
	dataVersionBuilder := array.NewStringBuilder(alloc)
	defer dataVersionBuilder.Release()
	specVersionBuilder := array.NewStringBuilder(alloc)
	defer specVersionBuilder.Release()
	dataSchemaBuilder := array.NewStringBuilder(alloc)
	defer dataSchemaBuilder.Release()
	signatureBuilder := array.NewStringBuilder(alloc)
	defer signatureBuilder.Release()
	extrasBuilder := array.NewStringBuilder(alloc)
	defer extrasBuilder.Release()
	dataBuilder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
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
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()

	return array.NewRecord(Schema, cols, int64(n)), nil
}
