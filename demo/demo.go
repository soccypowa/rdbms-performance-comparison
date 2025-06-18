package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/solontsev/rdbms-performance-comparison/config"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"

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

var testName = flag.String("test", "", "a name of the performance test to run")

func main() {
	var err error
	var db *sql.DB

	flag.Parse()
	if *testName == "" {
		log.Printf("Error: -name flag is required\n")
		flag.Usage()
		os.Exit(1)
	}

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
		{MariaDb, "mysql", "root:mariadb@tcp(127.0.0.1:3406)/test_db"},
		{MySql8, "mysql", "root:mysql@tcp(127.0.0.1:3308)/test_db?parseTime=true"},
		{MySql9, "mysql", "root:mysql@tcp(127.0.0.1:3307)/test_db?parseTime=true"},
		{PostgreSql16, "postgres", "postgres://postgres:postgres@localhost:5434/test_db?sslmode=disable"},
		{PostgreSql17, "postgres", "postgres://postgres:postgres@localhost:5433/test_db?sslmode=disable"},
		{PostgreSql18, "postgres", "postgres://postgres:postgres@localhost:5435/test_db?sslmode=disable"},
		{MsSql22, "sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1433?database=test_db"},
		{MsSql25, "sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1434?database=test_db"},
		//{MsSql19, "sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1434?database=test_db"},
	}

	result := make(map[string]map[string]string)
	debug := false

	for _, d := range databases {
		if debug {
			log.Printf("Running queries in %s database...", d.connectionName)
		}
		db, err = sql.Open(d.driverName, d.dsn)
		if err != nil {
			log.Fatalf("Unable to connect to database(%s): %v", d.connectionName, err)
		}

		db.SetConnMaxLifetime(0)
		db.SetMaxIdleConns(3)
		db.SetMaxOpenConns(3)
		Ping(ctx, db)

		testData, ok := Tests[*testName]
		if !ok {
			continue
		}

		queries, ok := testData.queries[d.connectionName]
		if !ok {
			continue
		}
		if debug {
			log.Printf("- %s - %s testName...", *testName, testData.testName)
		}

		//key := fmt.Sprintf("%s - %s", *testName, testData.testName)

		_, ok = result[d.connectionName]
		if !ok {
			result[d.connectionName] = make(map[string]string)
		}

		for queryName, sqlText := range queries {
			if debug {
				log.Printf("  - %s", queryName)
			}
			numberOfExecutions := config.TestExecutions
			if testData.execCount > 0 {
				numberOfExecutions = testData.execCount
			}
			duration := ExecQuery(ctx, testData.f, db, sqlText, numberOfExecutions)
			//subKey := d.connectionName
			//if queryName != "" {
			//	subKey += fmt.Sprintf(" (%s)", queryName)
			//}
			duration = duration.Round(time.Millisecond)
			result[d.connectionName][queryName] = fmt.Sprintf("%s", duration)
		}

		db.Close()
	}

	//prettyResult, err := json.MarshalIndent(result, "", "  ")
	//if err != nil {
	//	log.Println("error:", err)
	//}
	//
	//log.Println(string(prettyResult))
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	// headers
	columnNames := make([]string, 0, len(result)+1)
	for k := range result {
		columnNames = append(columnNames, k)
	}
	sort.Strings(columnNames)
	headerRow := make([]interface{}, len(columnNames)+1)
	headerRow[0] = ""
	for i, v := range columnNames {
		headerRow[i+1] = strings.Replace(v, "-", "\n", 1)
	}
	t.AppendHeader(headerRow)

	// data
	seen := make(map[string]struct{})
	rows := make([]string, 0)
	for _, r := range result {
		for queryName, _ := range r {
			if _, exists := seen[queryName]; !exists {
				seen[queryName] = struct{}{}
				rows = append(rows, queryName)
			}
		}
	}
	sort.Strings(rows)

	for _, r := range rows {
		row := make([]interface{}, len(columnNames)+1)
		prettyName := regexp.MustCompile(`^[a-z]\s+-\s+`).ReplaceAllString(r, "")
		row[0] = prettyName
		for i, v := range columnNames {
			v, _ := result[v][r]
			row[i+1] = v
		}
		t.AppendRow(row)
	}

	t.Render()
}
