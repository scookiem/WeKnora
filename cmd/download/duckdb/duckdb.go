package main

import (
	"context"
	"database/sql"

	_ "github.com/duckdb/duckdb-go/v2"
)

func downloadSpatial() {
	ctx := context.Background()

	sqlDB, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		panic(err)
	}

	// Try to install spatial extension (may already be installed or network unavailable)
	//在线下载 spatial.duckdb_extension
	//installSQL := "INSTALL spatial;"
	//离线安装
	installSQL := "INSTALL '/app/cmd/download/duckdb/spatial.duckdb_extension';"
	if _, err := sqlDB.ExecContext(ctx, installSQL); err != nil {
		panic(err)
	}

	// Try to load spatial extension
	loadSQL := "LOAD spatial;"
	if _, err := sqlDB.ExecContext(ctx, loadSQL); err != nil {
		panic(err)
	}
}

func main() {
	downloadSpatial()
}
