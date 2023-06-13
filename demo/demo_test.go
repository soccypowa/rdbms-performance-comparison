package demo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/solontsev/rdbms-performance-comparison/config"
	"log"
	"os"
	"os/signal"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
)

func Ping(ctx context.Context, db *sql.DB) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
}

func ExecQuery(ctx context.Context, f func(context.Context, *sql.DB, string), db *sql.DB, query string, execs int) time.Duration {
	for i := 0; i < config.WarmUpExecutions; i++ {
		f(ctx, db, query)
	}

	if execs == 0 {
		execs = config.TestExecutions
	}
	start := time.Now()
	for i := 0; i < execs; i++ {
		f(ctx, db, query)
	}
	return time.Since(start)
}

func TestDemo(t *testing.T) {
	var err error
	var db *sql.DB

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		<-appSignal
		stop()
	}()

	databases := []struct {
		connectionName string
		driverName     string
		dsn            string
	}{
		{"mysql", "mysql", "root:mysql@tcp(127.0.0.1:3306)/test_db"},
		{"postgres", "postgres", "postgres://postgres:postgres@localhost:5432/test_db?sslmode=disable"},
		{"mssql-22", "sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1433?database=test_db"},
		{"mssql-19", "sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1434?database=test_db"},
	}

	result := make(map[string]map[string]string, len(tests))
	for _, t := range tests {
		result[t.testName] = make(map[string]string)
	}

	for _, d := range databases {
		db, err = sql.Open(d.driverName, d.dsn)
		if err != nil {
			log.Fatalf("Unable to connect to database(%s): %v", d.connectionName, err)
		}

		db.SetConnMaxLifetime(0)
		db.SetMaxIdleConns(3)
		db.SetMaxOpenConns(3)
		Ping(ctx, db)

		for _, testData := range tests {
			queries, ok := testData.queries[d.connectionName]
			if !ok {
				continue
			} else {
				for queryName, sqlText := range queries {
					numberOfExecutions := config.TestExecutions
					if testData.execCount > 0 {
						numberOfExecutions = testData.execCount
					}
					duration := ExecQuery(ctx, testData.f, db, sqlText, numberOfExecutions)
					key := d.connectionName
					if queryName != "" {
						key += fmt.Sprintf("(%s)", queryName)
					}
					result[testData.testName][key] = fmt.Sprintf("%s", duration/time.Duration(config.TestExecutions))
				}
			}
		}

		db.Close()
	}

	prettyResult, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Println("error:", err)
	}

	log.Println(string(prettyResult))
}
