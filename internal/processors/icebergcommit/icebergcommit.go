// Package icebergcommit provides a Benthos processor that builds the Iceberg
// REST catalog update-table (append) request body from message metadata.
// It does not perform HTTP; a downstream output (e.g. http_client) POSTs the body.
//
// Snapshot ID is generated in this package (UUID-based int64); Bloblang cannot
// produce that value.
package icebergcommit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var configSpec = service.NewConfigSpec().
	Summary("Builds Iceberg REST update-table JSON from message metadata (no HTTP).")

func init() {
	err := service.RegisterProcessor("dimo_iceberg_commit", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(_ *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {
	return &requestBuilder{}, nil
}

type requestBuilder struct{}

// Close fulfills the service.Processor interface.
func (r *requestBuilder) Close(context.Context) error { return nil }

// Process reads dimo_parquet_* metadata, builds the Iceberg append request JSON,
// and sets the message body to that JSON (one data-file per message).
func (r *requestBuilder) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	path, ok := msg.MetaGet("dimo_parquet_path")
	if !ok || path == "" {
		return nil, fmt.Errorf("metadata dimo_parquet_path missing or empty")
	}

	sizeMeta, _ := msg.MetaGet("dimo_parquet_size")
	size := int64(0)
	if sizeMeta != "" {
		size, _ = strconv.ParseInt(sizeMeta, 10, 64)
	}

	countMeta, _ := msg.MetaGet("dimo_parquet_count")
	count := int64(0)
	if countMeta != "" {
		count, _ = strconv.ParseInt(countMeta, 10, 64)
	}

	body, err := buildCommitBody([]commitEntry{
		{FilePath: path, FileSize: size, RecordCount: count},
	})
	if err != nil {
		return nil, err
	}

	out := msg.Copy()
	out.SetBytes(body)
	return service.MessageBatch{out}, nil
}

type commitEntry struct {
	FilePath    string
	FileSize    int64
	RecordCount int64
}

// buildCommitBody returns the Iceberg REST update-table request JSON (append action).
// See: https://iceberg.apache.org/rest-catalog-spec/#update-a-table
func buildCommitBody(entries []commitEntry) ([]byte, error) {
	dataFiles := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		dataFiles = append(dataFiles, map[string]any{
			"file-path":          e.FilePath,
			"file-format":        "PARQUET",
			"record-count":       e.RecordCount,
			"file-size-in-bytes": e.FileSize,
		})
	}

	reqBody := map[string]any{
		"requirements": []map[string]any{},
		"updates": []map[string]any{
			{
				"action":       "append",
				"snapshot-id":  newSnapshotID(),
				"data-files":   dataFiles,
			},
		},
	}

	return json.Marshal(reqBody)
}

// newSnapshotID generates a unique snapshot ID for an Iceberg commit.
func newSnapshotID() int64 {
	u := uuid.New()
	var id int64
	for i := 0; i < 8; i++ {
		id = (id << 8) | int64(u[i])
	}
	if id < 0 {
		id = -id
	}
	return id
}
