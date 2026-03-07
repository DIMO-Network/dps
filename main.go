package main

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/clickhouse-infra/pkg/migrate"
	indexmigrations "github.com/DIMO-Network/cloudevent/clickhouse/migrations"
	sigmigrations "github.com/DIMO-Network/model-garage/pkg/migrations"
	"github.com/redpanda-data/benthos/v4/public/service"

	// Import aws for s3 output.
	_ "github.com/redpanda-data/connect/v4/public/components/aws"

	// Import sql for clickhouse output.
	_ "github.com/redpanda-data/connect/v4/public/components/sql"

	// Import io for http endpoints.
	_ "github.com/redpanda-data/connect/v4/public/components/io"

	// Import pure for basic processing.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	// Import prometheus for metrics.
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"

	// Add our custom plugin packages here.
	_ "github.com/DIMO-Network/dps/internal/processors/eventstoslice"
	_ "github.com/DIMO-Network/dps/internal/processors/parquet"
	_ "github.com/DIMO-Network/dps/internal/processors/signalstoslice"
	_ "github.com/DIMO-Network/dps/internal/processors/splitvalues"
)

func main() {
	host := envOrDefault("CLICKHOUSE_HOST", "localhost")
	port := envOrDefault("CLICKHOUSE_PORT", "9440")
	user := envOrDefault("CLICKHOUSE_USER", "default")
	pass := envOrDefault("CLICKHOUSE_PASSWORD", "")
	dimoDB := envOrDefault("CLICKHOUSE_DIMO_DATABASE", "dimo")
	indexDB := envOrDefault("CLICKHOUSE_INDEX_DATABASE", "dimo_index")

	dimoDSN := fmt.Sprintf("clickhouse://%s:%s/%s?username=%s&password=%s&secure=true&dial_timeout=5s", host, port, dimoDB, user, pass)
	indexDSN := fmt.Sprintf("clickhouse://%s:%s/%s?username=%s&password=%s&secure=true&dial_timeout=5s", host, port, indexDB, user, pass)

	runMigration("signal", dimoDSN, sigmigrations.BaseFS)
	runMigration("file_index", indexDSN, indexmigrations.BaseFS)

	service.RunCLI(context.Background())
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func runMigration(name, dsn string, baseFs fs.FS) {
	log.Printf("Running migration: %s", name)
	start := time.Now()
	dbOptions, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		log.Fatalf("Failed to parse DSN for %s: %v", name, err)
	}
	db := clickhouse.OpenDB(dbOptions)
	if err := migrate.RunGoose(context.Background(), []string{"up", "-v"}, baseFs, db); err != nil {
		_ = db.Close()
		log.Fatalf("Migration %s failed: %v", name, err)
	}
	if err := db.Close(); err != nil {
		log.Fatalf("Failed to close db after %s migration: %v", name, err)
	}
	log.Printf("Migration %s completed in %s", name, time.Since(start))
}
