package cloudeventtoparquet

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/pkg/clickhouse"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessBatch_MetadataAndPath(t *testing.T) {
	proc := &processor{prefix: "cloudevent/valid/"}
	defer func() { _ = proc.Close(context.Background()) }()

	msg := service.NewMessage([]byte(`{"id":"x","source":"s","producer":"p","specversion":"1.0","subject":"sub","time":"2025-01-01T00:00:00Z","type":"t"}`))
	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2) // 1 parquet + 1 original
	parquetMsg := batches[0][0]
	key, _ := parquetMsg.MetaGet(MetaS3UploadKey)
	require.NotEmpty(t, key)
	assert.Contains(t, key, "s/")
	assert.Regexp(t, `\d{4}/\d{2}/\d{2}/`, key, "path has year/month/day segments")
	assert.Contains(t, key, "batch-")
	assert.Contains(t, key, ".parquet")
	path, _ := parquetMsg.MetaGet(MetaParquetPath)
	assert.Equal(t, key, path)
}

func TestProcessBatch_GroupBySource(t *testing.T) {
	proc := &processor{prefix: "p/"}
	defer func() { _ = proc.Close(context.Background()) }()

	msg1 := service.NewMessage([]byte(`{"id":"a","source":"src1","producer":"p","specversion":"1.0","subject":"car1","time":"2025-01-01T00:00:00Z","type":"t"}`))
	msg2 := service.NewMessage([]byte(`{"id":"b","source":"src2","producer":"p","specversion":"1.0","subject":"car2","time":"2025-01-01T00:00:00Z","type":"t"}`))
	msg3 := service.NewMessage([]byte(`{"id":"c","source":"src1","producer":"p","specversion":"1.0","subject":"car1","time":"2025-01-01T00:00:00Z","type":"t"}`))
	batches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg1, msg2, msg3})
	require.NoError(t, err)
	require.Len(t, batches, 1)
	// 2 groups (src1, src2) -> 2 parquet msgs + 3 originals
	require.Len(t, batches[0], 5)
	keys := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		k, _ := batches[0][i].MetaGet(MetaS3UploadKey)
		keys = append(keys, k)
	}
	assert.Contains(t, keys[0], "src1/")
	assert.Contains(t, keys[1], "src2/")
	// Originals: all have dimo_cloudevent_index (parquet_path#row_offset)
	idx1, _ := batches[0][2].MetaGet(MetaCloudeventIndex)
	idx3, _ := batches[0][4].MetaGet(MetaCloudeventIndex)
	assert.Contains(t, idx1, "src1/")
	assert.Contains(t, idx3, "src1/")
	// Same file: index is path#offset, so prefix (path) should match for same group
	assert.Equal(t, idx1[:strings.Index(idx1, "#")], idx3[:strings.Index(idx3, "#")])
}

func TestSanitizePartitionValue(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"sub", "sub"},
		{"a/b", "a_b"},
		{"a\\b", "a_b"},
		{"", "_"},
		{"  ", "_"},
		{" /x/ ", "_x_"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := sanitizePartitionValue(tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestParquetToSplitValuesInsertStatements sends 50 mock CloudEvents through the parquet processor,
// then through split_values (with index_value meta set from each message body), and logs the
// resulting ClickHouse INSERT statements that sql_insert would send.
func TestParquetToSplitValuesInsertStatements(t *testing.T) {
	const numMessages = 50
	baseTime := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)

	// Build 50 mock CloudEvent messages (sources device/1 and device/2 for grouping).
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

	// 1. Run parquet processor
	proc := &processor{prefix: "ce/valid/"}
	defer func() { _ = proc.Close(context.Background()) }()
	batches, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	out := batches[0]

	// 2. Keep only originals (no dimo_s3_upload_key); set dimo_cloudevent_index_value for split_values
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

	// 3. Apply split_values logic (same as dimo_split_values processor) to get insert rows
	insertMsgs := runSplitValues(t, originals)
	require.Len(t, insertMsgs, numMessages)

	// 4. Format and log INSERT statements (table name matches stream: cloud_event_2)
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

// runSplitValues replicates dimo_split_values processor: reads dimo_cloudevent_index_value and dimo_cloudevent_index from each message, converts to clickhouse rows.
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

// formatInsertStmt returns a single-row INSERT statement for the clickhouse row (subject, event_time, event_type, id, source, producer, data_content_type, data_version, extras, index_key). cloudevent v0.1.6 returns 10 columns.
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
		return strconv.FormatFloat(x, 'f', -1, 64)
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
