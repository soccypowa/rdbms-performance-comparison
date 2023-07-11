package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/solontsev/rdbms-performance-comparison/config"
	"log"
	"os"
	"os/signal"
	"strings"
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

var tests = flag.String("tests", "01,02", "coma separated list of test keys")

func main() {
	var err error
	var db *sql.DB

	flag.Parse()

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
		{MySql, "mysql", "root:mysql@tcp(127.0.0.1:3306)/test_db"},
		{PostgreSql, "postgres", "postgres://postgres:postgres@localhost:5432/test_db?sslmode=disable"},
		{MsSql22, "sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1433?database=test_db"},
		//{MsSql19, "sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1434?database=test_db"},
	}

	var testCodes []string
	if *tests == "all" {
		testCodes = make([]string, len(Tests))
		i := 0
		for k := range Tests {
			testCodes[i] = k
			i++
		}
	} else {
		testCodes = strings.Split(*tests, ",")
		for i := range testCodes {
			testCodes[i] = strings.TrimSpace(testCodes[i])
		}
	}

	result := make(map[string]map[string]string, len(testCodes))

	for _, d := range databases {
		log.Printf("Running queries in %s database...", d.connectionName)
		db, err = sql.Open(d.driverName, d.dsn)
		if err != nil {
			log.Fatalf("Unable to connect to database(%s): %v", d.connectionName, err)
		}

		db.SetConnMaxLifetime(0)
		db.SetMaxIdleConns(3)
		db.SetMaxOpenConns(3)
		Ping(ctx, db)

		for _, testCode := range testCodes {
			testData, ok := Tests[testCode]
			if !ok {
				continue
			}

			queries, ok := testData.queries[d.connectionName]
			if !ok {
				continue
			}
			log.Printf("- %s - %s test...", testCode, testData.testName)

			key := fmt.Sprintf("%s - %s", testCode, testData.testName)
			_, ok = result[key]
			if !ok {
				result[key] = make(map[string]string)
			}

			for queryName, sqlText := range queries {
				log.Printf("  - %s", queryName)
				numberOfExecutions := config.TestExecutions
				if testData.execCount > 0 {
					numberOfExecutions = testData.execCount
				}
				duration := ExecQuery(ctx, testData.f, db, sqlText, numberOfExecutions)
				subKey := d.connectionName
				if queryName != "" {
					subKey += fmt.Sprintf(" (%s)", queryName)
				}
				result[key][subKey] = fmt.Sprintf("%s", duration)
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
