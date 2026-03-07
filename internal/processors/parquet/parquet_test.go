package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/clickhouse"
	pq "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newProcessor() *processor {
	logger := service.MockResources().Logger()
	return &processor{prefix: "cloudevent/valid/", logger: logger}
}

func mustParseEvent(t *testing.T, data []byte) cloudevent.RawEvent {
	t.Helper()
	var ev cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(data, &ev))
	return ev
}

// encodeEvents is a test helper that encodes events to parquet and returns bytes + index keys.
func encodeEvents(t *testing.T, events []cloudevent.RawEvent, objectKey string) ([]byte, map[int]string) {
	t.Helper()
	var buf bytes.Buffer
	indexKeys, err := pq.Encode(&buf, events, objectKey)
	require.NoError(t, err)
	return buf.Bytes(), indexKeys
}

// --- Processor tests ---

func TestProcessBatch_MetadataAndPath(t *testing.T) {
	proc := newProcessor()

	msg := service.NewMessage([]byte(`{"id":"x","source":"s","producer":"p","specversion":"1.0","subject":"sub","time":"2025-01-01T00:00:00Z","type":"t"}`))
	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2) // 1 parquet + 1 original
	parquetMsg := batches[0][0]
	key, _ := parquetMsg.MetaGet(MetaS3UploadKey)
	require.NotEmpty(t, key)
	assert.Regexp(t, `\d{4}/\d{2}/\d{2}/`, key, "path has year/month/day segments")
	assert.Contains(t, key, "batch-")
	assert.Contains(t, key, ".parquet")
	path, _ := parquetMsg.MetaGet(MetaParquetPath)
	assert.Equal(t, key, path)
}

func TestProcessBatch_OneParquetPerBatch(t *testing.T) {
	proc := &processor{prefix: "p/", logger: service.MockResources().Logger()}

	msg1 := service.NewMessage([]byte(`{"id":"a","source":"src1","producer":"p","specversion":"1.0","subject":"car1","time":"2025-01-01T00:00:00Z","type":"t"}`))
	msg2 := service.NewMessage([]byte(`{"id":"b","source":"src2","producer":"p","specversion":"1.0","subject":"car2","time":"2025-01-01T00:00:00Z","type":"t"}`))
	msg3 := service.NewMessage([]byte(`{"id":"c","source":"src1","producer":"p","specversion":"1.0","subject":"car1","time":"2025-01-01T00:00:00Z","type":"t"}`))
	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg1, msg2, msg3})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 4) // 1 parquet + 3 originals
	parquetMsg := batches[0][0]
	key, _ := parquetMsg.MetaGet(MetaS3UploadKey)
	require.NotEmpty(t, key)
	assert.Regexp(t, `\d{4}/\d{2}/\d{2}/`, key, "path has year/month/day (no source segment)")
	idx0, _ := batches[0][1].MetaGet(MetaCloudeventIndex)
	idx2, _ := batches[0][3].MetaGet(MetaCloudeventIndex)
	path0 := idx0[:strings.Index(idx0, "#")]
	path2 := idx2[:strings.Index(idx2, "#")]
	assert.Equal(t, path0, path2, "all originals reference same parquet file")
}

func TestProcessBatch_SkipsBadEvents(t *testing.T) {
	proc := newProcessor()

	good1 := service.NewMessage([]byte(`{"id":"a","source":"s","producer":"p","specversion":"1.0","subject":"sub","time":"2025-01-01T00:00:00Z","type":"t"}`))
	bad := service.NewMessage([]byte(`not valid json`))
	good2 := service.NewMessage([]byte(`{"id":"b","source":"s","producer":"p","specversion":"1.0","subject":"sub","time":"2025-01-01T00:00:00Z","type":"t"}`))

	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{good1, bad, good2})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 3) // 1 parquet + 2 good originals

	idx0, ok := batches[0][1].MetaGet(MetaCloudeventIndex)
	require.True(t, ok)
	assert.Contains(t, idx0, "#0")

	idx1, ok := batches[0][2].MetaGet(MetaCloudeventIndex)
	require.True(t, ok)
	assert.Contains(t, idx1, "#1")
}

func TestProcessBatch_AllBadEventsReturnsEmpty(t *testing.T) {
	proc := newProcessor()

	bad1 := service.NewMessage([]byte(`not json`))
	bad2 := service.NewMessage([]byte(`{broken`))

	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{bad1, bad2})
	require.NoError(t, err)
	require.Len(t, batches, 0)
}

func TestProcessBatch_EmptyBatch(t *testing.T) {
	proc := newProcessor()

	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 0)
}

// --- Encode/Decode tests ---

func TestEncode_SingleEvent(t *testing.T) {
	ev := mustParseEvent(t, []byte(`{
		"id": "ev-1",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data": "{\"k\":\"v\"}"
	}`))
	out, indexKeys := encodeEvents(t, []cloudevent.RawEvent{ev}, "test-key")
	require.NotEmpty(t, out)
	require.Len(t, indexKeys, 1)
	assert.Equal(t, "test-key#0", indexKeys[0])
}

func TestEncode_MultipleEvents(t *testing.T) {
	events := make([]cloudevent.RawEvent, 3)
	for i := range events {
		events[i] = mustParseEvent(t, []byte(`{
			"id": "ev-`+string(rune('a'+i))+`",
			"source": "test",
			"producer": "p",
			"specversion": "1.0",
			"subject": "sub",
			"time": "2025-01-01T00:00:00Z",
			"type": "dimo.status",
			"data": "{}"
		}`))
	}
	out, indexKeys := encodeEvents(t, events, "obj-key")
	require.NotEmpty(t, out)
	require.Len(t, indexKeys, 3)
	assert.Equal(t, "obj-key#0", indexKeys[0])
	assert.Equal(t, "obj-key#1", indexKeys[1])
	assert.Equal(t, "obj-key#2", indexKeys[2])
}

func TestEncode_RoundTrip(t *testing.T) {
	payload := []byte(`{
		"id": "roundtrip-1",
		"source": "test/source",
		"producer": "my-producer",
		"specversion": "1.0",
		"subject": "vehicle/vin123",
		"time": "2025-01-15T12:00:00.000Z",
		"type": "dimo.status",
		"datacontenttype": "application/json",
		"dataversion": "v1.2",
		"dataschema": "https://example.com/schema",
		"data": {"vin": "vin123", "speed": 42}
	}`)
	var original cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(payload, &original))

	parquetBytes, indexKeys := encodeEvents(t, []cloudevent.RawEvent{original}, "rt-key")
	require.NotEmpty(t, parquetBytes)
	require.Len(t, indexKeys, 1)

	reader := bytes.NewReader(parquetBytes)
	decoded, err := pq.Decode(reader, int64(reader.Len()))
	require.NoError(t, err)
	require.Len(t, decoded, 1)

	got := decoded[0]
	assert.Equal(t, original.ID, got.ID)
	assert.Equal(t, original.Source, got.Source)
	assert.Equal(t, original.Producer, got.Producer)
	assert.Equal(t, original.Subject, got.Subject)
	assert.Equal(t, original.Type, got.Type)
	assert.Equal(t, original.DataContentType, got.DataContentType)
	assert.Equal(t, original.DataVersion, got.DataVersion)
	assert.Equal(t, original.DataSchema, got.DataSchema)
	assert.Equal(t, original.Time.UnixMilli(), got.Time.UnixMilli())

	var origData, gotData any
	require.NoError(t, json.Unmarshal(original.Data, &origData))
	require.NoError(t, json.Unmarshal(got.Data, &gotData))
	assert.Equal(t, origData, gotData)
}

func TestEncode_DataBase64RoundTrip(t *testing.T) {
	ev := mustParseEvent(t, []byte(`{
		"id": "ev-b64",
		"source": "test",
		"producer": "p",
		"specversion": "1.0",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data_base64": "aGVsbG8="
	}`))
	parquetBytes, _ := encodeEvents(t, []cloudevent.RawEvent{ev}, "b64-key")

	reader := bytes.NewReader(parquetBytes)
	decoded, err := pq.Decode(reader, int64(reader.Len()))
	require.NoError(t, err)
	require.Len(t, decoded, 1)

	got := decoded[0]
	assert.Equal(t, "ev-b64", got.ID)
	assert.NotEmpty(t, got.DataBase64)
}

func TestEncode_SeekToRow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	events := []cloudevent.RawEvent{
		mustParseEvent(t, []byte(`{"id":"ev-0","source":"s","producer":"p","specversion":"1.0","subject":"sub","time":"`+now.Format(time.RFC3339Nano)+`","type":"dimo.status","data":"{}"}`)),
		mustParseEvent(t, []byte(`{"id":"ev-1","source":"s","producer":"p","specversion":"1.0","subject":"sub","time":"`+now.Format(time.RFC3339Nano)+`","type":"dimo.status","data":"{}"}`)),
	}

	parquetBytes, _ := encodeEvents(t, events, "seek-key")

	reader := bytes.NewReader(parquetBytes)
	got, err := pq.SeekToRow(reader, int64(reader.Len()), 1)
	require.NoError(t, err)
	assert.Equal(t, "ev-1", got.ID)
}

// --- Integration test: parquet → split_values ---

func TestParquetToSplitValuesInsertStatements(t *testing.T) {
	const numMessages = 50
	baseTime := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)

	msgs := make(service.MessageBatch, numMessages)
	for i := 0; i < numMessages; i++ {
		source := "oracle/1"
		if i >= 25 {
			source = "oracle/2"
		}
		body := fmt.Sprintf(`{"id":"ev-%d","source":"%s","producer":"p","specversion":"1.0","subject":"vehicle/%d","time":"%s","type":"dimo.status","data":"{}"}`,
			i+1, source, (i%10)+1, baseTime.Add(time.Duration(i)*time.Second).Format(time.RFC3339))
		msgs[i] = service.NewMessage([]byte(body))
	}

	proc := &processor{prefix: "ce/valid/", logger: service.MockResources().Logger()}
	batches, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	out := batches[0]

	var originals service.MessageBatch
	for _, m := range out {
		if _, ok := m.MetaGet(MetaS3UploadKey); ok {
			continue
		}
		_, ok := m.MetaGet(MetaCloudeventIndex)
		require.True(t, ok, "original must have dimo_cloudevent_index")
		b, err := m.AsBytes()
		require.NoError(t, err)
		var raw cloudevent.RawEvent
		require.NoError(t, json.Unmarshal(b, &raw))
		headersJSON, err := json.Marshal([]cloudevent.CloudEventHeader{raw.CloudEventHeader})
		require.NoError(t, err)
		m.MetaSetMut("dimo_cloudevent_index_value", string(headersJSON))
		originals = append(originals, m)
	}
	require.Len(t, originals, numMessages, "expected 50 originals")

	insertMsgs := runSplitValues(t, originals)
	require.Len(t, insertMsgs, numMessages)

	t.Logf("--- INSERT statements for %d rows (table cloud_event_2) ---", len(insertMsgs))
	for i, msg := range insertMsgs {
		vals, err := msg.AsStructured()
		require.NoError(t, err)
		row, ok := vals.([]any)
		require.True(t, ok)
		require.Len(t, row, 10)
		stmt := formatInsertStmt("cloud_event_2", row)
		t.Logf("%d: %s", i+1, stmt)
	}
	t.Logf("--- end INSERT statements ---")
}

func runSplitValues(t *testing.T, msgs service.MessageBatch) []*service.Message {
	t.Helper()
	var out []*service.Message
	for _, msg := range msgs {
		rawVal, ok := msg.MetaGetMut("dimo_cloudevent_index_value")
		require.True(t, ok, "dimo_cloudevent_index_value required")
		valuesStr, ok := rawVal.(string)
		require.True(t, ok, "dimo_cloudevent_index_value must be string")
		var hdrs []*cloudevent.CloudEventHeader
		require.NoError(t, json.Unmarshal([]byte(valuesStr), &hdrs))
		indexKey, ok := msg.MetaGet("dimo_cloudevent_index")
		require.True(t, ok, "dimo_cloudevent_index required")
		for _, hdr := range hdrs {
			vals := clickhouse.CloudEventToSliceWithKey(hdr, indexKey)
			m := msg.Copy()
			m.SetStructured(vals)
			out = append(out, m)
		}
	}
	return out
}

func formatInsertStmt(table string, row []any) string {
	cols := []string{"subject", "event_time", "event_type", "id", "source", "producer", "data_content_type", "data_version", "extras", "index_key"}
	var parts []string
	for _, v := range row {
		parts = append(parts, formatSQLValue(v))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		table, strings.Join(cols, ", "), strings.Join(parts, ", "))
}

func formatSQLValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case time.Time:
		return "'" + x.Format("2006-01-02 15:04:05") + "'"
	case int, int64, int32:
		return fmt.Sprintf("%d", x)
	case float64:
		return fmt.Sprintf("%g", x)
	case []string:
		if len(x) == 0 {
			return "[]"
		}
		parts := make([]string, len(x))
		for i, s := range x {
			parts[i] = "'" + strings.ReplaceAll(s, "'", "''") + "'"
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(x), "'", "''") + "'"
	}
}

// --- Benchmarks ---

func makeEvents(n int) []cloudevent.RawEvent {
	baseTime := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	events := make([]cloudevent.RawEvent, n)
	for i := range events {
		events[i] = cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				ID:              fmt.Sprintf("ev-%d", i),
				Source:          "oracle/1",
				Producer:        "test-producer",
				SpecVersion:     "1.0",
				Subject:         fmt.Sprintf("vehicle/%d", i%100),
				Time:            baseTime.Add(time.Duration(i) * time.Second),
				Type:            "dimo.status",
				DataContentType: "application/json",
			},
			Data: []byte(fmt.Sprintf(`{"speed":%d,"odometer":%d}`, 60+i%40, 10000+i*10)),
		}
	}
	return events
}

func BenchmarkProcessBatch(b *testing.B) {
	events := makeEvents(1000)
	// Pre-marshal events to JSON bytes for message payloads
	payloads := make([][]byte, len(events))
	for i, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			b.Fatal(err)
		}
		payloads[i] = data
	}

	proc := &processor{prefix: "cloudevent/valid/", logger: service.MockResources().Logger()}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		msgs := make(service.MessageBatch, len(payloads))
		for i, p := range payloads {
			msgs[i] = service.NewMessage(p)
		}
		_, err := proc.ProcessBatch(context.Background(), msgs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	for _, size := range []int{1, 10, 100, 200, 500} {
		events := makeEvents(size)
		b.Run(fmt.Sprintf("events_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var buf bytes.Buffer
				_, err := pq.Encode(&buf, events, "bench-key")
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, size := range []int{1, 10, 100, 200, 500} {
		events := makeEvents(size)
		var buf bytes.Buffer
		_, err := pq.Encode(&buf, events, "bench-key")
		if err != nil {
			b.Fatal(err)
		}
		encoded := buf.Bytes()
		b.Run(fmt.Sprintf("events_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				reader := bytes.NewReader(encoded)
				_, err := pq.Decode(reader, int64(reader.Len()))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
