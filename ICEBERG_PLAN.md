---
name: Iceberg Lakehouse Integration
overview: "Add Apache Iceberg to the DIMO data pipeline. Replace per-file S3 JSON CloudEvents with batched Iceberg Parquet tables. Use ClickHouse native TTL + S3-compatible storage policy for automatic signal/event cold tiering (no cron job). Cloud-agnostic: works on AWS, GCP, Azure, or on-prem with MinIO. Zero changes to telemetry-api."
todos:
  - id: dps-iceberg-output
    content: Custom Benthos output plugin in dps that batches CloudEvents into Parquet on object storage. Publishes file paths to a commit topic. Keeps ClickHouse cloud_event index insert with index_key as parquet_file#row_ref
    status: pending
  - id: dps-iceberg-committer
    content: New Benthos stream in dps that consumes the commit topic, batches file paths, and does single-writer Iceberg commits via iceberg-go + Lakekeeper. Eliminates commit contention.
    status: pending
  - id: dps-stream-config
    content: "Update valid-cloudevents.yaml and partial-cloudevents.yaml: replace aws_s3 JSON output with Parquet output + commit topic, keep ClickHouse index insert (dimo_split_values) with updated index_key format"
    status: pending
  - id: fetch-api-parquet
    content: "Update fetch-api to read payloads from Parquet files on object storage instead of individual JSON files: parse index_key as parquet_file#row_ref, use Go Parquet reader to read the full row"
    status: pending
  - id: ch-storage-policy
    content: Configure ClickHouse S3-compatible storage policy (cold volume) and add TTL MOVE TO VOLUME rules on signal/event tables for automatic tiering after 90 days
    status: pending
  - id: helm-config
    content: Add Iceberg/object storage env vars to values.yaml for all environments, update ClickHouse config for S3-compatible storage policy
    status: pending
  - id: iceberg-catalog
    content: Deploy Lakekeeper (Iceberg REST catalog) with PostgreSQL backend. Required from Phase 1 for the single Iceberg committer.
    status: pending
isProject: false
---

# Iceberg Lakehouse Integration for DIMO Data Pipeline

## Repos Involved

- **dps** -- data persistence service (Benthos streams, writes to ClickHouse + object storage)
- **dis** -- data ingest service (HTTP -> Kafka, unchanged by this work)
- **model-garage** -- VSS types, ClickHouse migrations, signal/event schemas
- **telemetry-api** -- GraphQL API that queries ClickHouse signal/event tables (UNCHANGED)
- **fetch-api** -- HTTP/gRPC API that serves raw CloudEvent payloads (currently from S3)

## Current Architecture

```mermaid
flowchart LR
    Devices --> DIS["DIS"]
    DIS --> Kafka
    Kafka --> DPS["DPS (Benthos)"]
    DPS -->|"signals/events"| CH["ClickHouse"]
    DPS -->|"1 JSON file per payload"| S3["Object Storage"]
    DPS -->|"index_key mapping"| CHIDX["ClickHouse cloud_event"]
    CH --> TelAPI["Telemetry API"]
    CHIDX -->|"lookup index_key"| FetchAPI["Fetch API"]
    FetchAPI -->|"GET by key"| S3
```



**Storage today:**

- `dimo.signal` -- ReplacingMergeTree, ORDER BY (token_id, timestamp, name)
- `dimo.event` -- ReplacingMergeTree, ORDER BY (subject, timestamp, name, source)
- `file_index.cloud_event` -- ReplacingMergeTree, maps header fields to `index_key` (object path)
- Object storage `dimo-ingest-cloudevent-{env}` -- one JSON file per CloudEvent payload
- Object storage `dimo-ingest-ephemeral-{env}` -- one JSON file per partial CloudEvent payload

**Problems:**

- Unbounded ClickHouse growth for signals/events
- Millions of tiny objects (per-request PUT costs + listing overhead)
- JSON is uncompressed/unqueryable
- AWS-locked (S3-specific bucket names, credentials)

## Target Architecture

```mermaid
flowchart LR
    Devices --> DIS["DIS"]
    DIS --> Kafka
    Kafka --> DPS["DPS (Benthos)"]
    DPS -->|"signals/events"| CH

    subgraph CH_group ["ClickHouse (single tables, tiered storage)"]
        CH["signal + event\nhot: local disk\ncold: object storage via TTL"]
        CHIDX["cloud_event index"]
    end

    DPS -->|"batched Parquet\nvia iceberg-go"| IcebergCE["Iceberg Table\ncloudevent/"]
    DPS -->|"index_key ref"| CHIDX
    IcebergCE --- Catalog["Lakekeeper\n(Iceberg REST Catalog)"]
    IcebergCE --- ObjStore["Object Storage\n(S3 / GCS / MinIO)"]
    CH --> TelAPI["Telemetry API\n(UNCHANGED)"]
    CHIDX -->|"metadata lookup"| FetchAPI["Fetch API"]
    FetchAPI -->|"Parquet range read"| ObjStore
```



**Key changes:**

- Individual JSON files replaced by batched Iceberg Parquet tables on object storage
- `cloud_event` ClickHouse index table kept as-is (fast O(log n) lookups)
- fetch-api reads payloads from Parquet files via range reads instead of GetObject
- signal/event cold tiering handled by ClickHouse native TTL + S3-compatible storage policy -- data automatically moves from local disk to object storage within the same table. No cron job, no external service, no code changes to telemetry-api
- Cloud-agnostic: Iceberg REST catalog + S3-compatible object storage runs on any cloud or on-prem

---

## Component 1: DPS -- Separated Write-Commit Architecture

**Repos:** [dps](dps/)

**Pattern:** Multiple DPS instances write Parquet files in parallel with zero coordination. A single Benthos committer stream (inside DPS) consumes file paths from Kafka and does batched Iceberg commits. This is the Adobe/Netflix pattern adapted to Benthos.

```mermaid
flowchart LR
    DPS1["DPS replica 1"] -->|"write Parquet"| ObjStore["Object Storage"]
    DPS2["DPS replica 2"] -->|"write Parquet"| ObjStore
    DPS3["DPS replica N"] -->|"write Parquet"| ObjStore
    DPS1 -->|"file path"| CommitTopic["Kafka: topic.iceberg.commits"]
    DPS2 -->|"file path"| CommitTopic
    DPS3 -->|"file path"| CommitTopic
    DPS1 -->|"index_key"| CH["ClickHouse cloud_event"]
    DPS2 -->|"index_key"| CH
    DPS3 -->|"index_key"| CH
    CommitTopic -->|"batched consume"| Committer["DPS Iceberg Committer\n(single Benthos stream)"]
    Committer -->|"batched Iceberg append"| Lakekeeper["Lakekeeper"]
```



**Part A: Parquet Writer (runs on every DPS replica)**

**Location:** `dps/internal/outputs/parquetwriter/`

A custom Benthos output plugin that:

- Accepts batches from Benthos (configured via `batching:` in stream YAML)
- Encodes as a single Parquet file using `apache/arrow-go` with a unique filename (`batch-{uuid}.parquet`)
- Writes Parquet file to object storage (no coordination -- each instance writes to a unique path)
- Publishes the file path + metadata to `topic.iceberg.commits` on Kafka
- Returns the Parquet file path + row offset per message so the downstream ClickHouse index insert can store it as `index_key`
- No Iceberg commit happens here -- just a Parquet file write and a Kafka message

**Parquet schema (full CloudEvent header + payload):**

```
-- Identity
id                  String          -- unique event ID
subject             String          -- vehicle DID (did:erc721:137:0x...:42)
source              String          -- oracle DID
producer            String          -- device DID

-- Temporal
time                Timestamp(us)   -- event timestamp

-- Classification
type                String          -- dimo.status, dimo.fingerprint, etc.
data_content_type   String          -- MIME type
data_version        String          -- schema version

-- Metadata
specversion         String          -- CloudEvents spec version
dataschema          String          -- schema URI
signature           String          -- data field signature
tags                List<String>    -- filterable tags
extras              String          -- JSON blob for remaining arbitrary fields

-- Payload
data                Binary          -- the raw payload (opaque blob, can be large -- video files)
```

Header fields are tiny relative to `data` -- negligible storage overhead. Promotes the archive to a self-contained, analytically queryable dataset. Parquet dictionary encoding compresses low-cardinality fields (`type`, `specversion`, `data_content_type`) extremely well. Column statistics on `subject`, `type`, `time` enable efficient scans from Spark/Trino/DuckDB without the ClickHouse index.

**Part B: Iceberg Committer (single Benthos stream, one instance)**

**Location:** `dps/charts/dps/files/streams/iceberg-committer.yaml`

A new Benthos stream that:

- Consumes `topic.iceberg.commits` from Kafka (consumer group ensures single consumer)
- Batches file paths (e.g., accumulate for 5-10 seconds or 100 files)
- Does a single Iceberg append commit via `iceberg-go` REST catalog client -> Lakekeeper
- Commits Kafka offset after successful Iceberg commit

Zero contention -- only one process ever touches Iceberg metadata. If it falls behind, it just batches more files per commit (bigger batches are more efficient in Iceberg).

**Failure modes:**

- Committer crashes: Kafka retains file paths, committer catches up on restart. Parquet files already exist on object storage. Worst case: a window where files exist but aren't in the Iceberg table. Fetch-api still works because it reads by `index_key` (Parquet file path), not by scanning the Iceberg table.
- Committer lag: Not a problem for reads. `index_key` in ClickHouse points directly to Parquet files. The Iceberg table is eventually consistent with what's on disk -- only matters for full table scans (analytics), not point reads.

**Stream YAML for valid-cloudevents.yaml:**

```yaml
output:
  broker:
    pattern: fan_out_sequential_fail_fast
    outputs:
      # 1. Write Parquet to object storage + publish file path to commit topic
      - label: "write_valid_cloudevent_parquet"
        dimo_parquet_writer:
          warehouse: ${ICEBERG_WAREHOUSE}
          prefix: cloudevent/valid/
          storage_endpoint: ${OBJECT_STORAGE_ENDPOINT}
          credentials:
            access_key: ${OBJECT_STORAGE_ACCESS_KEY}
            secret_key: ${OBJECT_STORAGE_SECRET_KEY}
          commit_topic: ${ICEBERG_COMMIT_TOPIC:topic.iceberg.commits}
          kafka_brokers: ${KAFKA_BOOTSTRAP_SERVERS}:${KAFKA_BOOTSTRAP_PORT}
          batching:
            count: 10000
            period: "5s"

      # 2. Insert index into ClickHouse cloud_event table
      - label: "insert_valid_cloudevent_clickhouse"
        sql_insert:
          driver: clickhouse
          dsn: ...
          table: cloud_event
          columns: []
          args_mapping: root = this
          batching:
            count: 500000
            period: "2s"
        processors:
          - label: "split_values_valid"
            dimo_split_values: {}
```

**Stream YAML for iceberg-committer.yaml (new stream, single instance):**

```yaml
input:
  kafka_franz:
    seed_brokers:
      - ${KAFKA_BOOTSTRAP_SERVERS}:${KAFKA_BOOTSTRAP_PORT}
    topics:
      - ${ICEBERG_COMMIT_TOPIC:topic.iceberg.commits}
    consumer_group: "zone.dimo.dps.iceberg-committer"

pipeline:
  processors:
    - label: "batch_and_commit"
      dimo_iceberg_commit:
        catalog_uri: ${ICEBERG_CATALOG_URI}
        warehouse: ${ICEBERG_WAREHOUSE}
        batching:
          count: 100
          period: "10s"
```

**Key change to `index_key`:** Instead of an object path like `a{subject}!{time}!{type}!{source}!{id}`, it now stores a Parquet reference: `cloudevent/valid/year=2025/month=06/day=15/batch-{uuid}.parquet#{row_offset}`.

**Changes to [cloudevent/pkg/clickhouse/clickhouse.go](cloudevent/pkg/clickhouse/clickhouse.go):**

- The Parquet writer output plugin sets the `dimo_cloudevent_index` metadata with the new key format, and `dimo_split_values` picks it up as before

Same pattern for [partial-cloudevents.yaml](dps/charts/dps/files/streams/partial-cloudevents.yaml).

**What gets removed from DPS:**

- `aws_s3` output for CloudEvents

**What gets added to DPS:**

- `dimo_parquet_writer` output plugin (writes Parquet, publishes to commit topic)
- `dimo_iceberg_commit` processor (batched Iceberg commits via Lakekeeper)
- `iceberg-committer.yaml` stream definition

**What stays in DPS:**

- `dimo_split_values` processor
- ClickHouse `cloud_event` index insert
- The `fan_out_sequential_fail_fast` broker pattern
- Signal and event ClickHouse inserts (unchanged)

**Dependencies to add:** `github.com/apache/iceberg-go`, `github.com/apache/arrow-go/v18`

---

## Component 2: ClickHouse `cloud_event` Index Table -- Kept As-Is

**No migration needed.** The `cloud_event` table in `file_index` database stays exactly as it is:

```sql
-- Existing table, unchanged
CREATE TABLE cloud_event (
    subject String,
    event_time DateTime64(3, 'UTC'),
    event_type String,
    id String,
    source String,
    producer String,
    data_content_type String,
    data_version String,
    extras String,
    index_key String  -- now points to parquet_file#row_ref instead of object key
)
ENGINE = ReplacingMergeTree()
ORDER BY (subject, event_time, event_type, source, id)
```

The only change is **what `index_key` contains** -- a Parquet file reference instead of an individual object path. The table schema, ORDER BY, and all query patterns remain identical.

**Why keep it:** The ORDER BY `(subject, event_time, event_type, source, id)` gives O(log n) lookups for the exact query patterns fetch-api uses. Iceberg partition pruning + Parquet row group stats cannot match this for high-cardinality point lookups or open-ended "latest" queries.

---

## Component 3: Fetch API -- Parquet Payload Reader

**Repos:** [fetch-api](fetch-api/)

**Current flow** ([eventrepo.go](fetch-api/pkg/eventrepo/eventrepo.go)):

1. Query `cloud_event` table for header metadata + `index_key`
2. `s3.GetObject(bucket, index_key)` to fetch raw JSON payload
3. Combine header + payload into response

**New flow (parallel migration -- both paths run simultaneously):**

1. Query `cloud_event` table for header metadata + `index_key` **(unchanged)**
2. Route based on `index_key` format:
  - If `index_key` contains `#` -> **Iceberg path:** parse as `{parquet_path}#{row_ref}`, read `data` column from Parquet file via range read
  - Otherwise -> **Legacy S3 path:** `s3.GetObject(bucket, index_key)` as today
3. Combine header + payload into response

This allows both old (per-file JSON) and new (Iceberg Parquet) CloudEvents to coexist. No big-bang migration. DPS starts writing new CloudEvents to Iceberg while all existing data remains accessible via S3.

```mermaid
sequenceDiagram
    participant Client
    participant FetchAPI as FetchAPI
    participant CH as ClickHouse
    participant ObjStore as ObjectStorage

    Client->>FetchAPI: GET /v1/vehicle/latest-object/{tokenId}
    FetchAPI->>CH: SELECT metadata + index_key FROM cloud_event WHERE ...
    CH-->>FetchAPI: headers + index_key
    alt index_key contains #
        FetchAPI->>ObjStore: Parquet range read (data column)
    else legacy key
        FetchAPI->>ObjStore: S3 GetObject (JSON file)
    end
    ObjStore-->>FetchAPI: raw payload bytes
    FetchAPI-->>Client: CloudEvent with header + data
```



**Changes in [eventrepo.go](fetch-api/pkg/eventrepo/eventrepo.go):**

- `GetObjectFromKey()` becomes a router: checks `index_key` format, dispatches to Parquet reader or legacy S3 GetObject
- New internal package `pkg/parquetreader/` that:
  - Parses `index_key` into Parquet file path + row identifier
  - Opens the Parquet file via S3-compatible range reads (no full download)
  - Reads the full row (the original complete JSON CloudEvent object, not just the data field)
  - For small payloads: returns bytes in memory
  - For large payloads (video): streams to client
- Legacy S3 GetObject path stays intact for old `index_key` values
- `ListCloudEventsFromIndexes()` batches reads from the same Parquet file (multiple rows often land in the same batch file)
- Multi-bucket fallback logic kept for legacy keys; new Iceberg keys use the Iceberg warehouse bucket

**What stays the same in fetch-api:**

- ClickHouse query logic (same table, same columns, same filters)
- `SearchOptions`, `AdvancedSearchOptions` query building
- gRPC and HTTP handler interfaces
- All index query patterns

**What changes:**

- `GetObjectFromKey` implementation (GetObject -> Parquet range read)
- `index_key` parsing logic
- Object storage config (single Iceberg warehouse endpoint instead of 3 fallback buckets)

**New dependency:** `github.com/apache/arrow-go/v18` for Parquet reading from S3-compatible storage

---

## Component 4: ClickHouse Native TTL + S3-Compatible Storage Policy (replaces cron job)

**No custom code. No cron job. No new services.** ClickHouse handles signal/event cold tiering automatically via its built-in TTL MOVE TO VOLUME feature. Works with any S3-compatible storage (AWS S3, GCS, MinIO).

### How it works

ClickHouse supports multi-volume storage policies. Data parts are automatically moved between volumes based on TTL rules evaluated during merge operations.

**Step 1: Configure S3-compatible storage as a cold volume in ClickHouse config:**

```xml
<storage_configuration>
  <disks>
    <default/>
    <object_storage_cold>
      <type>s3</type>
      <!-- Works with AWS S3, GCS (S3-compat), MinIO, etc. -->
      <endpoint>${CLICKHOUSE_COLD_STORAGE_ENDPOINT}</endpoint>
      <access_key_id>${CLICKHOUSE_COLD_ACCESS_KEY}</access_key_id>
      <secret_access_key>${CLICKHOUSE_COLD_SECRET_KEY}</secret_access_key>
    </object_storage_cold>
  </disks>
  <policies>
    <tiered>
      <volumes>
        <hot>
          <disk>default</disk>
        </hot>
        <cold>
          <disk>object_storage_cold</disk>
        </cold>
      </volumes>
    </tiered>
  </policies>
</storage_configuration>
```

**Step 2: Apply storage policy and TTL to signal/event tables:**

New migration in [model-garage](model-garage/) (`pkg/migrations/`), e.g., `00007_tiered_storage.sql`:

```sql
-- +goose Up
ALTER TABLE signal MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE signal MODIFY TTL timestamp + INTERVAL 90 DAY TO VOLUME 'cold';

ALTER TABLE event MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE event MODIFY TTL timestamp + INTERVAL 90 DAY TO VOLUME 'cold';
```

**What happens automatically:**

- Data younger than 90 days lives on local disk (hot volume) -- fast
- During ClickHouse merge operations (checked every ~4 hours by default), parts with data older than 90 days are moved to object storage (cold volume)
- **The table remains one table.** All queries work identically -- FINAL, PREWHERE, argMax, toStartOfInterval, pointInPolygon -- ClickHouse transparently reads from local disk or object storage
- No union views. No archive tables. No query routing. No code changes to telemetry-api.

**Tuning:**

- `merge_with_ttl_timeout` controls how often TTL is evaluated (default: 14400 seconds / 4 hours)
- Can be lowered for faster tiering if needed
- ClickHouse disk cache can be enabled for the cold volume to cache frequently accessed cold data

---

## Component 5: Iceberg REST Catalog -- Lakekeeper

Required from Phase 1. The single Iceberg committer writes through Lakekeeper, which serializes commits in PostgreSQL and provides credential vending.

[Lakekeeper](https://lakekeeper.io/) ([github](https://github.com/lakekeeper/lakekeeper)) -- single-binary Rust Iceberg REST catalog. No JVM.

**Deployment:**

```yaml
# Minimal env vars for Lakekeeper
LAKEKEEPER__PG_DATABASE_URL_WRITE: postgres://postgres:password@postgres:5432/lakekeeper
LAKEKEEPER__PG_ENCRYPTION_KEY: (from secret)
LAKEKEEPER__LISTEN_PORT: "8181"
LAKEKEEPER__METRICS_PORT: "9000"

# Storage -- credential vending for S3-compatible object storage
LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS: "true"  # or configure per-warehouse

# Auth (optional, recommended for production)
LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION: "true"
LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE: https://kubernetes.default.svc
```

- Helm chart from `lakekeeper/lakekeeper`, deploy as ClusterIP service at `http://lakekeeper:8181`
- Requires PostgreSQL >= 15 (can share existing or deploy dedicated)
- Built-in Swagger UI at `/swagger-ui`, web UI at `/` for table browsing
- Prometheus metrics on port 9000

**Integration with DPS (iceberg-go):**

`iceberg-go` connects to Lakekeeper as a standard REST catalog:

```go
catalog, _ := catalog.NewRestCatalog("rest", map[string]string{
    "uri":       "http://lakekeeper:8181",
    "warehouse": "cloudevent",
})
```

Lakekeeper vends short-term storage credentials to `iceberg-go` -- the DPS output plugin never needs direct object storage keys.

**Integration with Tansu (future Phase 3):**

Tansu connects via the same REST catalog endpoint: `--iceberg-catalog http://lakekeeper:8181`.

**Built-in table maintenance:**

Lakekeeper runs background task workers for snapshot expiration, soft-deleted table cleanup, and file purge on drop. This handles Iceberg metadata cleanup automatically -- no external compaction needed for the CloudEvent tables.

---

## Component 6: Helm / Configuration

**Repos:** [dps](dps/) charts, ClickHouse infrastructure config

**New env vars** in [values.yaml](dps/charts/dps/values.yaml) and [values-prod.yaml](dps/charts/dps/values-prod.yaml):

```yaml
# Iceberg / Object Storage (cloud-agnostic)
ICEBERG_CATALOG_URI: http://iceberg-rest-catalog:8181
ICEBERG_WAREHOUSE: s3://dimo-iceberg-{env}/warehouse/
OBJECT_STORAGE_ENDPOINT: https://s3.us-east-2.amazonaws.com  # or MinIO/GCS endpoint
OBJECT_STORAGE_ACCESS_KEY: (from secret)
OBJECT_STORAGE_SECRET_KEY: (from secret)

# ClickHouse cold tier (cloud-agnostic)
CLICKHOUSE_COLD_STORAGE_ENDPOINT: https://dimo-clickhouse-cold-{env}.s3.us-east-2.amazonaws.com/data/
CLICKHOUSE_COLD_ACCESS_KEY: (from secret)
CLICKHOUSE_COLD_SECRET_KEY: (from secret)
```

**ClickHouse config changes** (infrastructure-level, not in app repos):

- Add S3-compatible cold disk definition
- Add tiered storage policy
- These are ClickHouse server config, typically managed via Helm chart for ClickHouse itself

**New Kubernetes resources:**

- Lakekeeper deployment (Helm chart from `lakekeeper/lakekeeper`)

**Env vars to eventually remove** (after CloudEvent migration complete):

- `S3_CLOUDEVENT_BUCKET`
- `S3_EPHEMERAL_BUCKET` (from fetch-api)
- `S3_AWS_REGION`, `S3_AWS_ACCESS_KEY_ID`, `S3_AWS_SECRET_ACCESS_KEY` (replaced by generic object storage vars)

---

## ClickHouse Table Summary (Final State)

**Signal/Event tables (tiered automatically via TTL):**

- `dimo.signal` -- ReplacingMergeTree, storage_policy='tiered', TTL 90d to cold (object storage)
- `dimo.event` -- ReplacingMergeTree, storage_policy='tiered', TTL 90d to cold (object storage)
- `dimo.signal_last_state` -- ReplacingMergeTree (unchanged)
- `dimo.signal_state_changes` -- ReplacingMergeTree (unchanged)

**CloudEvent index (kept permanently on local disk):**

- `file_index.cloud_event` -- ReplacingMergeTree (unchanged schema, `index_key` now points to Parquet refs)

**Iceberg Tables on Object Storage (CloudEvent payloads):**

- `cloudevent.valid` -- Iceberg table, Parquet files on object storage (filesystem catalog initially, REST catalog after Lakekeeper)
- `cloudevent.partial` -- Iceberg table, Parquet files on object storage (filesystem catalog initially, REST catalog after Lakekeeper)

**ClickHouse native format on Object Storage (auto-tiered signal/event data):**

- Cold volume path -- ClickHouse MergeTree parts (managed by ClickHouse, not user-accessible)

**Object storage buckets deprecated (after migration complete):**

- `dimo-ingest-cloudevent-{env}` -- individual JSON files, no longer written
- `dimo-ingest-ephemeral-{env}` -- individual JSON files, no longer written

---

## Implementation Phases

**Phase 1 -- CloudEvent Storage Migration (per-file JSON -> Iceberg Parquet)**

1. Deploy Lakekeeper (Iceberg REST catalog) with PostgreSQL backend
2. Build Parquet writer output plugin in DPS (writes Parquet to object storage, publishes file paths to Kafka commit topic)
3. Build Iceberg committer Benthos stream in DPS (single consumer, batched commits via `iceberg-go` -> Lakekeeper)
4. Update DPS stream configs: replace `aws_s3` with Parquet writer + commit topic, keep ClickHouse index insert
5. Update `index_key` format to reference Parquet files
6. Update fetch-api payload retrieval: Parquet range reads for new keys, legacy S3 GetObject for old keys

**Phase 2 -- Signal/Event Cold Tiering (biggest storage savings, zero code)**
7. Configure ClickHouse S3-compatible cold disk and tiered storage policy
8. Apply TTL MOVE TO VOLUME on signal and event tables via migration
9. Verify telemetry-api works unchanged (it will -- same table, same engine)

**Phase 3 -- Cleanup**
10. Delete old JSON files from deprecated CloudEvent/ephemeral buckets
11. Remove old bucket config and AWS-specific env vars from services

---

## Risks and Mitigations

- **ClickHouse S3-compatible storage policy support:** Requires self-managed ClickHouse (or ClickHouse Cloud Scale/Enterprise). Works with any S3-compatible endpoint (AWS S3, GCS, MinIO). Verify deployment supports custom storage policies.
- **TTL timing:** TTL moves happen during merges, not immediately. Default check interval is ~4 hours (`merge_with_ttl_timeout`). Data may linger on hot storage slightly beyond 90 days. This is acceptable.
- **Object storage query latency for cold signals:** Queries touching data > 90 days will be slower (network latency). Mitigated by ClickHouse disk cache on the cold volume. Most telemetry-api queries hit recent data.
- **Fetch API latency change:** Payload retrieval changes from GetObject (whole JSON file) to Parquet range read (single column, single row). Comparable latency for small payloads; better for large payloads since only the `data` column is read.
- **Large payloads in Parquet:** Video files in Parquet row groups inflate file sizes. Mitigate by using separate row groups per message when `data` exceeds a size threshold, or by using no compression on the `data` binary column.
- **iceberg-go maturity:** Official Apache project at v0.4.0. Pre-1.0 but actively developed with REST catalog and append support. Fallback: write raw Parquet + commit via REST catalog API directly.
- **Iceberg small files:** No compaction on CloudEvent tables (would break `index_key` references). Lakekeeper's built-in snapshot expiration keeps metadata from growing unbounded. Fetch-api does point reads by `index_key`, not table scans, so small files don't affect read performance. If analytics queries over the archive are added later, compaction can be introduced with a lookup-by-`id` path at that point.
- **ClickHouse S3 replication:** If using replicated tables, avoid `allow_s3_zero_copy_replication` (experimental, known bugs). Use standard replication with separate paths per replica.

---

## Future Roadmap: Kafka Replacement and DPS Elimination

The current pipeline uses Kafka purely as a buffer between DIS (which does all processing) and DPS (which only sinks to storage). This is over-engineered for a buffering role.

### Phase 3 (future): Replace Kafka with Tansu

[Tansu](https://tansu.io) is an open-source Kafka-compatible broker with built-in Iceberg sink support. It can automatically convert schema-backed topics into Iceberg tables on any S3-compatible storage.

**Target state:**

```mermaid
flowchart LR
    Devices --> DIS["DIS"]
    DIS --> Tansu["Tansu (Kafka-compatible)"]
    Tansu -->|"auto Iceberg sink"| IcebergCE["Iceberg Tables\ncloudevent/"]
    Tansu --> DPS["DPS (Benthos)"]
    DPS -->|"signals/events"| CH["ClickHouse\ntiered to object storage"]
    CH --> TelAPI["Telemetry API"]
    CH -->|"index lookup"| FetchAPI["Fetch API"]
    FetchAPI -->|"Parquet range read"| IcebergCE
```



**What changes:**

- Replace Kafka deployment with Tansu (Kafka-compatible, drop-in for existing producers/consumers)
- Configure Tansu's Iceberg sink for CloudEvent topics -> Iceberg tables on object storage
- Remove the custom Benthos Iceberg output plugin from DPS (Tansu handles it)
- DPS still consumes signal/event topics and writes to ClickHouse

**What stays the same:**

- DIS output code unchanged (Tansu is Kafka-compatible)
- DPS Kafka consumption unchanged (Tansu is Kafka-compatible)
- All Iceberg tables and query paths from Phase 1 remain
- Fetch API and Telemetry API unchanged

**Benefit:** Eliminates the custom Iceberg writer we build in Phase 1. Tansu handles schema -> Iceberg conversion natively. Also handles compaction, eliminating the small files problem.

### Phase 4 (future): Collapse DPS into DIS

Once Tansu handles the CloudEvent Iceberg sink, DPS's remaining job is:

1. Consume `topic.device.signals` -> write to ClickHouse `signal` table
2. Consume `topic.device.events` -> write to ClickHouse `event` table

These are simple sink operations. They can be moved directly into DIS as additional Benthos outputs (DIS is already a Benthos service).

**Target state (final):**

```mermaid
flowchart LR
    Devices --> DIS["DIS (ingest + sink)"]
    DIS -->|"signals/events"| CH["ClickHouse\ntiered to object storage"]
    DIS -->|"CloudEvents via Tansu"| Tansu["Tansu"]
    Tansu -->|"auto Iceberg sink"| IcebergCE["Iceberg Tables\ncloudevent/"]
    CH --> TelAPI["Telemetry API"]
    CH -->|"index lookup"| FetchAPI["Fetch API"]
    FetchAPI -->|"Parquet range read"| IcebergCE
```



**What changes:**

- DIS output section adds ClickHouse `sql_insert` outputs for signals and events (currently in DPS)
- DPS service is decommissioned entirely

**Benefit:** Eliminates an entire service (DPS) and its Kubernetes deployment. DIS already does the heavy processing. Adding ClickHouse writes is trivial.

### Phasing Summary


| Phase      | Scope                                  | Services Changed                   | New Infrastructure                      |
| ---------- | -------------------------------------- | ---------------------------------- | --------------------------------------- |
| 1 (now)    | CloudEvent payloads -> Iceberg Parquet | dps, fetch-api                     | Lakekeeper + PG, object storage bucket  |
| 2 (now)    | Signal/event cold tiering              | None (config only)                 | ClickHouse S3-compatible storage policy |
| 3 (future) | Replace Kafka with Tansu               | Infrastructure only                | Tansu (replaces Kafka + DPS committer)  |
| 4 (future) | Merge DPS into DIS                     | dis (gains outputs), dps (removed) | None                                    |


