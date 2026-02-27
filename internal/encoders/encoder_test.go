package encoders

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaFields(t *testing.T) {
	expected := []string{
		"id", "subject", "source", "producer", "time", "type",
		"data_content_type", "data_version", "dataschema",
		"tags", "extras", "data", "data_base64",
	}
	require.Equal(t, len(expected), Schema.NumFields())
	for i, name := range expected {
		require.Equal(t, name, Schema.Field(i).Name, "field %d", i)
	}
}

func TestEncodeToParquet_SingleEvent(t *testing.T) {
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data": "{\"k\":\"v\"}"
	}`)
	out, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, out)
}

func TestEncodeToParquet_WithTags(t *testing.T) {
	payload := []byte(`{
		"id": "ev-tags",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"tags": ["a", "b", "c"],
		"data": "{}"
	}`)
	out, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, out)
	// Schema has tags as list of string
	listType, ok := Schema.Field(colTags).Type.(*arrow.ListType)
	require.True(t, ok, "tags column is list type")
	require.True(t, arrow.TypeEqual(listType.Elem(), arrow.BinaryTypes.String), "tags element type is string")
}

func TestEncodeToParquet_EmptyFails(t *testing.T) {
	_, err := EncodeToParquet(nil)
	require.Error(t, err)
	_, err = EncodeToParquet([][]byte{})
	require.Error(t, err)
}

// TestRawEvent_DataBase64 ensures RawEvent unmarshals data_base64 and populates Data.
// Encoder uses RawEvent; this confirms base64-encoded event data is accepted in the pipeline.
func TestRawEvent_DataBase64(t *testing.T) {
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data_base64": "YmluYXJ5AHBheWxvYWQ="
	}`)
	var rawEvent cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(payload, &rawEvent))
	assert.Equal(t, []byte("binary\x00payload"), []byte(rawEvent.Data))
}

// parquetRow holds the first row read from a Parquet table (CloudEvent columns).
type parquetRow struct {
	ID              string
	Subject         string
	Source          string
	Producer        string
	Time            time.Time
	Type            string
	DataContentType string
	DataVersion     string
	DataSchema      string
	Tags            []string
	Extras          string
	Data            string
	DataBase64      []byte
}

// readParquetFirstRow reads the first row from Parquet bytes into a parquetRow.
func readParquetFirstRow(t *testing.T, parquetBytes []byte) parquetRow {
	t.Helper()
	mem := memory.NewGoAllocator()
	pf, err := file.NewParquetReader(bytes.NewReader(parquetBytes), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()
	require.GreaterOrEqual(t, tbl.NumRows(), int64(1), "expected at least one row")
	row := parquetRow{}
	getStr := func(colIdx int) string {
		col := tbl.Column(colIdx)
		chunks := col.Data().Chunks()
		if len(chunks) == 0 || chunks[0].NullN() == chunks[0].Len() {
			return ""
		}
		arr := chunks[0].(*array.String)
		return arr.Value(0)
	}
	getStrNull := func(colIdx int) (string, bool) {
		col := tbl.Column(colIdx)
		chunks := col.Data().Chunks()
		if len(chunks) == 0 || chunks[0].NullN() == 1 {
			return "", false
		}
		arr := chunks[0].(*array.String)
		return arr.Value(0), true
	}
	getBinaryNull := func(colIdx int) ([]byte, bool) {
		col := tbl.Column(colIdx)
		chunks := col.Data().Chunks()
		if len(chunks) == 0 || chunks[0].NullN() == 1 {
			return nil, false
		}
		arr := chunks[0].(*array.Binary)
		return arr.Value(0), true
	}
	getListStr := func(colIdx int) []string {
		col := tbl.Column(colIdx)
		chunks := col.Data().Chunks()
		if len(chunks) == 0 {
			return nil
		}
		arr := chunks[0].(*array.List)
		if arr.NullN() == 1 {
			return nil
		}
		vals := arr.ListValues().(*array.String)
		offsets := arr.Offsets()
		start, end := int(offsets[0]), int(offsets[1])
		out := make([]string, 0, end-start)
		for i := start; i < end; i++ {
			out = append(out, vals.Value(i))
		}
		return out
	}
	getTime := func(colIdx int) time.Time {
		col := tbl.Column(colIdx)
		chunks := col.Data().Chunks()
		if len(chunks) == 0 {
			return time.Time{}
		}
		arr := chunks[0].(*array.Timestamp)
		ts := arr.Value(0)
		return time.UnixMicro(int64(ts)).UTC()
	}
	row.ID = getStr(colID)
	row.Subject = getStr(colSubject)
	row.Source = getStr(colSource)
	row.Producer = getStr(colProducer)
	row.Time = getTime(colTime)
	row.Type = getStr(colType)
	row.DataContentType = getStr(colDataContentType)
	row.DataVersion = getStr(colDataVersion)
	row.DataSchema = getStr(colDataSchema)
	row.Tags = getListStr(colTags)
	row.Extras = getStr(colExtras)
	if v, ok := getStrNull(colData); ok {
		row.Data = v
	}
	if v, ok := getBinaryNull(colDataBase64); ok {
		row.DataBase64 = v
	}
	return row
}

// parquetRowToCloudEventJSON converts a parquetRow to JSON bytes in CloudEvent shape.
func parquetRowToCloudEventJSON(t *testing.T, row parquetRow) []byte {
	t.Helper()
	m := map[string]any{
		"specversion":       "1.0",
		"id":                row.ID,
		"source":            row.Source,
		"subject":           row.Subject,
		"producer":          row.Producer,
		"time":              row.Time.Format(time.RFC3339Nano),
		"type":              row.Type,
		"data_content_type": row.DataContentType,
	}
	if row.DataVersion != "" {
		m["data_version"] = row.DataVersion
	}
	if row.DataSchema != "" {
		m["dataschema"] = row.DataSchema
	}
	if len(row.Tags) > 0 {
		m["tags"] = row.Tags
	}
	if row.Data != "" {
		var dataVal any
		if err := json.Unmarshal([]byte(row.Data), &dataVal); err != nil {
			m["data"] = json.RawMessage(row.Data)
		} else {
			m["data"] = dataVal
		}
	}
	if len(row.DataBase64) > 0 {
		m["data_base64"] = base64.StdEncoding.EncodeToString(row.DataBase64)
	}
	if row.Extras != "" && row.Extras != "{}" {
		var extras map[string]any
		if err := json.Unmarshal([]byte(row.Extras), &extras); err == nil {
			for k, v := range extras {
				if k == "tags" {
					continue
				}
				m[k] = v
			}
		}
	}
	out, err := json.Marshal(m)
	require.NoError(t, err)
	return out
}

// readParquetDataColumn reads the first row's data column from Parquet bytes.
// Returns (value, true) if non-null, ("", false) if null.
func readParquetDataColumn(t *testing.T, parquetBytes []byte) (string, bool) {
	t.Helper()
	mem := memory.NewGoAllocator()
	pf, err := file.NewParquetReader(bytes.NewReader(parquetBytes), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()
	require.Equal(t, int64(1), tbl.NumRows(), "expected one row")
	dataCol := tbl.Column(colData)
	require.NotNil(t, dataCol)
	chunks := dataCol.Data().Chunks()
	require.GreaterOrEqual(t, len(chunks), 1, "data column has at least one chunk")
	arr := chunks[0]
	require.Equal(t, 1, arr.Len())
	if arr.NullN() == 1 {
		return "", false
	}
	strArr, ok := arr.(*array.String)
	require.True(t, ok, "data column is string")
	return strArr.Value(0), true
}

// readParquetDataBase64Column reads the first row's data_base64 column from Parquet bytes.
// Returns (value, true) if non-null, (nil, false) if null.
func readParquetDataBase64Column(t *testing.T, parquetBytes []byte) ([]byte, bool) {
	t.Helper()
	mem := memory.NewGoAllocator()
	pf, err := file.NewParquetReader(bytes.NewReader(parquetBytes), file.WithReadProps(parquet.NewReaderProperties(mem)))
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()
	require.Equal(t, int64(1), tbl.NumRows(), "expected one row")
	dataBase64Col := tbl.Column(colDataBase64)
	require.NotNil(t, dataBase64Col)
	chunks := dataBase64Col.Data().Chunks()
	require.GreaterOrEqual(t, len(chunks), 1, "data_base64 column has at least one chunk")
	arr := chunks[0]
	require.Equal(t, 1, arr.Len())
	if arr.NullN() == 1 {
		return nil, false
	}
	binArr, ok := arr.(*array.Binary)
	require.True(t, ok, "data_base64 column is binary")
	return binArr.Value(0), true
}

// TestEncodeToParquet_DataBase64RoundTrip encodes an event with only data_base64,
// reads the Parquet back, and asserts data column is null and data_base64 column has decoded bytes.
func TestEncodeToParquet_DataBase64RoundTrip(t *testing.T) {
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data_base64": "YmluYXJ5AHBheWxvYWQ="
	}`)
	parquetBytes, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, parquetBytes)
	_, ok := readParquetDataColumn(t, parquetBytes)
	assert.False(t, ok, "data column should be null when event had data_base64")
	dataBase64Val, ok64 := readParquetDataBase64Column(t, parquetBytes)
	require.True(t, ok64, "data_base64 column should be non-null when event had data_base64")
	assert.Equal(t, []byte("binary\x00payload"), dataBase64Val)
}

// TestEncodeToParquet_DataOnly_DataBase64Null encodes an event with only "data" (no data_base64),
// reads the Parquet back, and asserts data column has the string and data_base64 column is null.
func TestEncodeToParquet_DataOnly_DataBase64Null(t *testing.T) {
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data": "{\"k\":\"v\"}"
	}`)
	parquetBytes, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, parquetBytes)
	dataVal, okData := readParquetDataColumn(t, parquetBytes)
	require.True(t, okData, "data column should be non-null when event had inline data")
	// rawEvent.Data is the raw JSON value (string includes surrounding quotes)
	assert.Equal(t, "\"{\\\"k\\\":\\\"v\\\"}\"", dataVal)
	_, ok := readParquetDataBase64Column(t, parquetBytes)
	assert.False(t, ok, "data_base64 column should be null when event used data field only")
}

// TestEncodeToParquet_JSONData_StoredAsString verifies that when a CloudEvent has inline JSON data,
// the Parquet file stores it in the "data" column as string (not binary).
func TestEncodeToParquet_JSONData_StoredAsString(t *testing.T) {
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data": {"foo": "bar", "n": 42}
	}`)
	parquetBytes, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, parquetBytes)
	// Schema: data column must be string type
	require.True(t, arrow.TypeEqual(Schema.Field(colData).Type, arrow.BinaryTypes.String),
		"data column in schema must be string type")
	// Read back: data column is string and contains the JSON
	dataVal, ok := readParquetDataColumn(t, parquetBytes)
	require.True(t, ok, "data column should be non-null")
	assert.Equal(t, `{"foo": "bar", "n": 42}`, dataVal, "data column must store JSON as string (exact bytes from payload)")
}

// TestEncodeToParquet_DataBase64_StoredAsBinaryNotBase64String verifies that when a CloudEvent has
// data_base64, the Parquet file stores the decoded payload in the "data_base64" column as binary,
// not the base64-encoded string.
func TestEncodeToParquet_DataBase64_StoredAsBinaryNotBase64String(t *testing.T) {
	base64Payload := "YmluYXJ5AHBheWxvYWQ=" // base64("binary\x00payload")
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data_base64": "` + base64Payload + `"
	}`)
	parquetBytes, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, parquetBytes)
	// Schema: data_base64 column must be binary type
	require.True(t, arrow.TypeEqual(Schema.Field(colDataBase64).Type, arrow.BinaryTypes.Binary),
		"data_base64 column in schema must be binary type")
	// Read back: data_base64 column is binary and contains decoded bytes, not the base64 string
	decodedVal, ok := readParquetDataBase64Column(t, parquetBytes)
	require.True(t, ok, "data_base64 column should be non-null")
	assert.Equal(t, []byte("binary\x00payload"), decodedVal, "data_base64 column must store decoded bytes")
	assert.NotEqual(t, []byte(base64Payload), decodedVal, "data_base64 column must not store base64 string")
}

// TestEncodeToParquet_NoDataNull encodes an event with no data and no data_base64,
// reads the Parquet back, and asserts both data and data_base64 columns are null.
func TestEncodeToParquet_NoDataNull(t *testing.T) {
	payload := []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status"
	}`)
	parquetBytes, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, parquetBytes)
	_, ok := readParquetDataColumn(t, parquetBytes)
	assert.False(t, ok, "data column should be null when event has no data or data_base64")
	_, ok64 := readParquetDataBase64Column(t, parquetBytes)
	assert.False(t, ok64, "data_base64 column should be null when event has no data_base64")
}

// TestEncodeToParquet_RoundTrip_JSONBack takes a standard CloudEvent payload, encodes to Parquet,
// reads the Parquet back into a row, converts to CloudEvent JSON, and asserts all previous fields are present.
func TestEncodeToParquet_RoundTrip_JSONBack(t *testing.T) {
	payload := []byte(`{
		"id": "roundtrip-1",
		"source": "test/source",
		"producer": "my-producer",
		"specversion": "1.0",
		"subject": "vehicle/vin123",
		"time": "2025-01-15T12:00:00.000Z",
		"type": "dimo.status",
		"data_content_type": "application/json",
		"data_version": "1.2",
		"dataschema": "https://example.com/schema",
		"tags": ["tag1", "tag2"],
		"data": {"vin": "vin123", "speed": 42}
	}`)
	var original cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(payload, &original))

	parquetBytes, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, parquetBytes)

	row := readParquetFirstRow(t, parquetBytes)
	jsonBack := parquetRowToCloudEventJSON(t, row)

	var roundtripped cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(jsonBack, &roundtripped))

	assert.Equal(t, original.ID, roundtripped.ID)
	assert.Equal(t, original.Source, roundtripped.Source)
	assert.Equal(t, original.Producer, roundtripped.Producer)
	assert.Equal(t, original.Subject, roundtripped.Subject)
	assert.Equal(t, original.Type, roundtripped.Type)
	assert.Equal(t, original.DataContentType, roundtripped.DataContentType)
	assert.Equal(t, original.DataVersion, roundtripped.DataVersion)
	assert.Equal(t, original.DataSchema, roundtripped.DataSchema)
	assert.Equal(t, original.Tags, roundtripped.Tags)
	assert.Equal(t, original.Time.UnixMicro(), roundtripped.Time.UnixMicro())
	assert.Equal(t, original.DataBase64, roundtripped.DataBase64)
	// Data may re-encode with different key order/whitespace; compare semantically
	var origData, rtData any
	require.NoError(t, json.Unmarshal(original.Data, &origData))
	require.NoError(t, json.Unmarshal(roundtripped.Data, &rtData))
	assert.Equal(t, origData, rtData)
}

// TestEncodeToParquet_RoundTrip_JSONBack_DataBase64 is the same as RoundTrip_JSONBack but uses data_base64.
func TestEncodeToParquet_RoundTrip_JSONBack_DataBase64(t *testing.T) {
	payload := []byte(`{
		"id": "roundtrip-b64",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data_content_type": "application/octet-stream",
		"tags": ["binary"],
		"data_base64": "YmluYXJ5AHBheWxvYWQ="
	}`)
	var original cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(payload, &original))

	parquetBytes, err := EncodeToParquet([][]byte{payload})
	require.NoError(t, err)
	require.NotEmpty(t, parquetBytes)

	row := readParquetFirstRow(t, parquetBytes)
	jsonBack := parquetRowToCloudEventJSON(t, row)

	var roundtripped cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(jsonBack, &roundtripped))

	assert.Equal(t, original.ID, roundtripped.ID)
	assert.Equal(t, original.Source, roundtripped.Source)
	assert.Equal(t, original.Subject, roundtripped.Subject)
	assert.Equal(t, original.Type, roundtripped.Type)
	assert.Equal(t, original.Tags, roundtripped.Tags)
	assert.Equal(t, []byte("binary\x00payload"), []byte(roundtripped.Data), "data_base64 round-trips as decoded Data")
	assert.Equal(t, original.DataBase64, roundtripped.DataBase64)
}
