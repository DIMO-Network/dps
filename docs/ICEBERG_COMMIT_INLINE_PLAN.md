# Plan: Inline Iceberg commit in cloudevent output

Move the Iceberg catalog commit from a separate Kafka consumer (iceberg-committer) into the valid/partial cloudevent pipeline outputs. Each pipeline POSTs to the Lakekeeper REST API directly after writing Parquet to S3.

## Implementation (completed)

The implementation uses the **built-in `http_client` output** and a single **sync processor** `dimo_iceberg_commit`:

- **Processor** (`internal/processors/icebergcommit/`): Reads `dimo_parquet_path`, `dimo_parquet_size`, `dimo_parquet_count` from message metadata and config `namespace`/`table_name`. Builds the Iceberg REST update-table (append) JSON body (including unique snapshot ID). Sets the message body to that JSON. No HTTP; no shared client package.
- **Stream YAML:** The second fallback branch in valid-cloudevents-2 and partial-cloudevents-2 uses `dimo_iceberg_commit` (processors) + `http_client` (url, verb POST, headers, timeout). No Kafka commit topic.
- **Removed:** iceberg-committer.yaml, `ICEBERG_COMMIT_TOPIC` from values, topic.iceberg.commits from Kafka topic list. ICEBERG_PLAN.md updated.

---

## Goals

- Remove the commit topic and iceberg-committer stream.
- One less component to run and monitor; simpler topology.
- Same REST API behavior; request volume remains low (e.g. 10 instances × 5s batches).

## Out of scope

- Changing ClickHouse or S3 write paths.
- Changing the Iceberg REST request shape or Lakekeeper integration.

---

## 1. Shared commit logic (internal package)

**Path:** `internal/icebergcommit/client.go` (new package, not under `processors/`).

- Extract from current `icebergcommit` processor:
  - `CommitMessage` struct (or equivalent with `file_path`, `file_size`, `record_count`, `namespace`, `table_name`).
  - `commitFiles(ctx, namespace, tableName, commits []CommitMessage)` HTTP logic: build JSON body, POST to `/v1/{warehouse}/namespaces/{namespace}/tables/{table}`, handle non-2xx.
  - `newSnapshotID()` helper.
- No Benthos types in this package; plain `*http.Client`, `context`, and structs. This makes it testable and reusable by both the new output and (temporarily) the old processor.
- Optional: add a small unit test that mocks the HTTP round trip and asserts request URL/body and error handling.

**Deliverable:** Package `github.com/DIMO-Network/dps/internal/icebergcommit` with `Commit(ctx, catalogURI, warehouse string, commits []CommitMessage) error` (or per-table `CommitTable(...)`) used by the output and, during migration, by the processor.

---

## 2. New Benthos output: `dimo_iceberg_commit`

**Path:** `internal/outputs/icebergcommit/icebergcommit.go`.

- Register with `service.RegisterOutput("dimo_iceberg_commit", configSpec, ctor)`.
- Config fields (same as current processor): `catalog_uri`, `warehouse`.
- Output behavior:
  - For each batch, read from each message: either JSON body (current commit-topic shape) or metadata (`dimo_parquet_path`, `dimo_parquet_size`, `dimo_parquet_count`) + fixed or configured `namespace`/`table_name`. Prefer metadata so the stream can pass through the same message shape as today without an extra mapping step.
  - Group by `namespace` + `table_name` (same as current processor).
  - For each group, call shared `icebergcommit.Commit(...)` (or commit-per-table helper).
  - On success: pass batch through (output acks). On error: return error so Benthos can retry.
- Use the shared package for HTTP; no duplication of request building.
- Wire a `*service.Logger` for “Committed N files to namespace.table” style logs.

**Deliverable:** Output plugin `dimo_iceberg_commit` that accepts the same logical input as the current processor (either JSON body or metadata) and calls Lakekeeper.

---

## 3. Register output in main.go

- Add: `_ "github.com/DIMO-Network/dps/internal/outputs/icebergcommit"`.
- Keep the existing `icebergcommit` processor import until the processor is removed (so existing stream configs keep working until cutover).

---

## 4. Update valid-cloudevents-2 and partial-cloudevents-2 streams

**Files:** `charts/dps/files/streams/valid-cloudevents-2.yaml`, `charts/dps/files/streams/partial-cloudevents-2.yaml`.

- In the output broker, replace the second fallback branch (current “publish to iceberg commit topic”) with the new output.
- Current pattern:
  - Branch 2: switch on `dimo_parquet_path` set and `dimo_s3_upload_key` empty → `kafka_franz` to `ICEBERG_COMMIT_TOPIC` with mapping `root.file_path = metadata("dimo_parquet_path")`, etc. → else reject.
- New pattern:
  - Branch 2: same switch → `dimo_iceberg_commit` output with `catalog_uri` and `warehouse` from env (e.g. `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE`). No Kafka. Message content can stay as-is; output reads from metadata (and optionally body). Else same reject path.
- Ensure the output receives only messages that have Parquet metadata (same condition as today). Keep the fallback/error labels and `handle_db_error` behavior for the reject branch.

**Deliverable:** Both streams commit to Iceberg directly in-pipeline; no publish to Kafka for commits.

---

## 5. Refactor old processor to use shared package (optional but recommended)

- In `internal/processors/icebergcommit/icebergcommit.go`: remove local `commitFiles` and request-building code; depend on `internal/icebergcommit` and call the shared function from `ProcessBatch`. This keeps one implementation of the REST contract and eases removal of the processor later.

---

## 6. Remove iceberg-committer stream and processor

- **Delete** `charts/dps/files/streams/iceberg-committer.yaml`. Streams are globbed from `files/streams/*`, so deletion is enough to stop deploying it.
- **Remove** the `dimo_iceberg_commit` batch processor:
  - Delete `internal/processors/icebergcommit/` (or leave a tiny shim that delegates to the output’s logic if you prefer a single code path; the plan assumes full removal).
  - Remove from `main.go`: `_ "github.com/DIMO-Network/dps/internal/processors/icebergcommit"`.
- **Config/values:** Remove or repurpose `ICEBERG_COMMIT_TOPIC` from `values.yaml` and `values-prod.yaml` (no longer used). Ensure `ICEBERG_CATALOG_URI` and `ICEBERG_WAREHOUSE` are set where the new output runs (likely already present for the old committer).

---

## 7. Docs and cleanup

- **ICEBERG_PLAN.md:** Update architecture: no commit topic; no dps-iceberg-committer stream; valid/partial pipelines show “Iceberg commit” as an output step (HTTP to Lakekeeper). Update any sequence diagrams or stream lists.
- **Topic:** If nothing else uses `topic.iceberg.commits`, document or add a follow-up to drop the topic after deploy and verification.

---

## 8. Testing and rollout

- **Unit:** Shared package and output tests (mock HTTP); existing processor test, if kept during transition, should pass using shared package.
- **Integration:** Run valid/partial pipelines against dev Lakekeeper; confirm Parquet files appear in the table and that no traffic goes to the commit topic.
- **Rollout:** Deploy with new streams; confirm commits succeed. Then remove iceberg-committer stream (and processor) and topic references. No need to drain the commit topic if no new messages are produced.

---

## Order of implementation

| Step | Action |
|------|--------|
| 1 | Add `internal/icebergcommit` with shared HTTP commit logic (and optional tests). |
| 2 | Add `internal/outputs/icebergcommit` output; register in `main.go`. |
| 3 | Refactor `internal/processors/icebergcommit` to use shared package (optional). |
| 4 | Update `valid-cloudevents-2.yaml` and `partial-cloudevents-2.yaml` to use `dimo_iceberg_commit` output instead of Kafka. |
| 5 | Delete `iceberg-committer.yaml`; remove processor and its import; clean env/docs. |
| 6 | Update ICEBERG_PLAN.md and run tests. |

---

## Rollback

If issues appear: re-add the iceberg-committer stream and the processor; revert valid/partial stream YAML to publish to `ICEBERG_COMMIT_TOPIC` again. No schema or Lakekeeper changes required.
