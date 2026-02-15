// Package icebergcommit provides a Benthos batch processor that commits
// Parquet file paths to an Iceberg table via the Iceberg REST catalog API.
//
// This processor runs as a single-instance consumer on the iceberg commit topic.
// It accumulates file paths and does batched Iceberg append commits via direct
// REST API calls to Lakekeeper (no iceberg-go dependency).
package icebergcommit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// CommitMessage is the JSON structure published to the commit topic by the parquet writer.
type CommitMessage struct {
	// FilePath is the object storage key of the Parquet file.
	FilePath string `json:"file_path"`
	// FileSize is the size of the Parquet file in bytes.
	FileSize int64 `json:"file_size"`
	// RecordCount is the number of records in the Parquet file.
	RecordCount int64 `json:"record_count"`
	// Namespace is the Iceberg namespace (e.g. "cloudevent").
	Namespace string `json:"namespace"`
	// TableName is the Iceberg table name (e.g. "valid" or "partial").
	TableName string `json:"table_name"`
}

var configSpec = service.NewConfigSpec().
	Summary("Commits batched Parquet file paths to an Iceberg table via REST catalog.").
	Field(service.NewStringField("catalog_uri").Description("URI of the Iceberg REST catalog (e.g. http://lakekeeper:8181).")).
	Field(service.NewStringField("warehouse").Description("Iceberg warehouse identifier."))

func init() {
	err := service.RegisterBatchProcessor("dimo_iceberg_commit", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	catalogURI, err := conf.FieldString("catalog_uri")
	if err != nil {
		return nil, fmt.Errorf("field catalog_uri: %w", err)
	}
	warehouse, err := conf.FieldString("warehouse")
	if err != nil {
		return nil, fmt.Errorf("field warehouse: %w", err)
	}

	return &committer{
		catalogURI: catalogURI,
		warehouse:  warehouse,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		logger:     mgr.Logger(),
	}, nil
}

type committer struct {
	catalogURI string
	warehouse  string
	httpClient *http.Client
	logger     *service.Logger
}

// Close fulfills the service.BatchProcessor interface.
func (c *committer) Close(context.Context) error { return nil }

// ProcessBatch receives a batch of commit messages and appends the referenced
// Parquet files to the appropriate Iceberg table via the REST catalog API.
func (c *committer) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msgs) == 0 {
		return []service.MessageBatch{msgs}, nil
	}

	// Group commit messages by namespace.table.
	type tableKey struct{ namespace, table string }
	grouped := make(map[tableKey][]CommitMessage)

	for i, msg := range msgs {
		payload, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("message %d: get bytes: %w", i, err)
		}

		var cm CommitMessage
		if err := json.Unmarshal(payload, &cm); err != nil {
			return nil, fmt.Errorf("message %d: unmarshal commit message: %w", i, err)
		}

		key := tableKey{namespace: cm.Namespace, table: cm.TableName}
		grouped[key] = append(grouped[key], cm)
	}

	// Commit each table's files via REST API.
	for key, commits := range grouped {
		if err := c.commitFiles(ctx, key.namespace, key.table, commits); err != nil {
			return nil, fmt.Errorf("commit to %s.%s: %w", key.namespace, key.table, err)
		}
		c.logger.Infof("Committed %d Parquet files to %s.%s", len(commits), key.namespace, key.table)
	}

	return []service.MessageBatch{msgs}, nil
}

// commitFiles appends Parquet files to an Iceberg table via the REST catalog
// update-table API endpoint.
//
// See: https://iceberg.apache.org/rest-catalog-spec/#update-a-table
func (c *committer) commitFiles(ctx context.Context, namespace, tableName string, commits []CommitMessage) error {
	// Build the append-files update request per the Iceberg REST spec.
	// The REST API uses the "append" update with data-file entries.
	dataFiles := make([]map[string]any, 0, len(commits))
	for _, cm := range commits {
		dataFiles = append(dataFiles, map[string]any{
			"file-path":        cm.FilePath,
			"file-format":      "PARQUET",
			"record-count":     cm.RecordCount,
			"file-size-in-bytes": cm.FileSize,
			// partition and column-sizes/value-counts can be added later for optimization.
		})
	}

	reqBody := map[string]any{
		"requirements": []map[string]any{},
		"updates": []map[string]any{
			{
				"action": "append",
				"snapshot-id": newSnapshotID(),
				"data-files":  dataFiles,
			},
		},
	}

	bodyJSON, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	// POST to /v1/{prefix}/namespaces/{namespace}/tables/{table}
	url := fmt.Sprintf("%s/v1/%s/namespaces/%s/tables/%s", c.catalogURI, c.warehouse, namespace, tableName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyJSON))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("catalog returned %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// newSnapshotID generates a unique snapshot ID for an Iceberg commit.
func newSnapshotID() int64 {
	u := uuid.New()
	// Use the first 8 bytes as a positive int64.
	var id int64
	for i := 0; i < 8; i++ {
		id = (id << 8) | int64(u[i])
	}
	if id < 0 {
		id = -id
	}
	return id
}
