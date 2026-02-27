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
	{Name: "data_content_type", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "data_version", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "dataschema", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	{Name: "extras", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "data_base64", Type: arrow.BinaryTypes.Binary, Nullable: true},
}, nil)

// EncodeToParquet parses each JSON payload as a CloudEvent (RawEvent), builds an Arrow record,
// and encodes it to Parquet. Returns the Parquet bytes.
func EncodeToParquet(payloads [][]byte) ([]byte, error) {
	if len(payloads) == 0 {
		return nil, fmt.Errorf("no payloads")
	}
	headers, jsonData, base64Data, err := parsePayloads(payloads)
	if err != nil {
		return nil, err
	}
	batch, err := buildRecordBatch(headers, jsonData, base64Data)
	if err != nil {
		return nil, err
	}
	defer batch.Release()
	var buf bytes.Buffer
	pqWriter, err := pqarrow.NewFileWriter(Schema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}
	if err := pqWriter.Write(batch); err != nil {
		return nil, fmt.Errorf("write parquet record: %w", err)
	}
	if err := pqWriter.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

func parsePayloads(payloads [][]byte) ([]*cloudevent.CloudEventHeader, [][]byte, []string, error) {
	headers := make([]*cloudevent.CloudEventHeader, 0, len(payloads))
	jsonData := make([][]byte, 0, len(payloads))
	base64Data := make([]string, 0, len(payloads))
	for i, payload := range payloads {
		var rawEvent cloudevent.RawEvent
		if err := json.Unmarshal(payload, &rawEvent); err != nil {
			return nil, nil, nil, fmt.Errorf("payload %d: unmarshal cloudevent: %w", i, err)
		}
		headers = append(headers, &rawEvent.CloudEventHeader)
		jsonData = append(jsonData, rawEvent.Data)
		base64Data = append(base64Data, rawEvent.DataBase64)
	}
	return headers, jsonData, base64Data, nil
}

// Schema column indices for appendRow (order matches Schema field order).
const (
	colID, colSubject, colSource, colProducer = 0, 1, 2, 3
	colTime, colType                          = 4, 5
	colDataContentType, colDataVersion        = 6, 7
	colDataSchema, colTags, colExtras         = 8, 9, 10
	colData, colDataBase64                    = 11, 12
)

func appendRow(bldr *array.RecordBuilder, hdr *cloudevent.CloudEventHeader, data []byte, dataBase64 string) {
	bldr.Field(colID).(*array.StringBuilder).Append(hdr.ID)
	bldr.Field(colSubject).(*array.StringBuilder).Append(hdr.Subject)
	bldr.Field(colSource).(*array.StringBuilder).Append(hdr.Source)
	bldr.Field(colProducer).(*array.StringBuilder).Append(hdr.Producer)
	bldr.Field(colTime).(*array.TimestampBuilder).Append(arrow.Timestamp(hdr.Time.UnixMicro()))
	bldr.Field(colType).(*array.StringBuilder).Append(hdr.Type)
	bldr.Field(colDataContentType).(*array.StringBuilder).Append(hdr.DataContentType)
	bldr.Field(colDataVersion).(*array.StringBuilder).Append(hdr.DataVersion)
	bldr.Field(colDataSchema).(*array.StringBuilder).Append(hdr.DataSchema)
	appendTags(bldr.Field(colTags).(*array.ListBuilder), hdr.Tags)
	bldr.Field(colExtras).(*array.StringBuilder).Append(marshalExtras(hdr))
	dataBldr := bldr.Field(colData).(*array.StringBuilder)
	if dataBase64 == "" && data != nil {
		dataBldr.Append(string(data))
	} else {
		dataBldr.AppendNull()
	}
	dataBase64Bldr := bldr.Field(colDataBase64).(*array.BinaryBuilder)
	if dataBase64 != "" && data != nil {
		dataBase64Bldr.Append(data)
	} else {
		dataBase64Bldr.AppendNull()
	}
}

func appendTags(bldr *array.ListBuilder, tags []string) {
	bldr.Append(true)
	sb := bldr.ValueBuilder().(*array.StringBuilder)
	for _, t := range tags {
		sb.Append(t)
	}
}

func marshalExtras(hdr *cloudevent.CloudEventHeader) string {
	extras := make(map[string]any, len(hdr.Extras)+1)
	for k, v := range hdr.Extras {
		extras[k] = v
	}
	if len(hdr.Tags) > 0 {
		extras["tags"] = hdr.Tags
	}
	b, err := json.Marshal(extras)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func buildRecordBatch(headers []*cloudevent.CloudEventHeader, rawData [][]byte, dataBase64 []string) (arrow.RecordBatch, error) {
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, Schema)
	defer bldr.Release()

	for i, hdr := range headers {
		appendRow(bldr, hdr, rawData[i], dataBase64[i])
	}

	return bldr.NewRecordBatch(), nil
}
