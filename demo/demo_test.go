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

func Query01(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var id int
	err := db.QueryRowContext(ctx, query).Scan(&id)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func Query02(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var id int
	var name string
	err := db.QueryRowContext(ctx, query).Scan(&id, &name)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func Query03(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var count int
	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func Query05(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int
	err := db.QueryRowContext(ctx, query).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
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
		{"mssql2022", "sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1433?database=test_db"},
	}

	data := []struct {
		testName  string
		queries   map[string]map[string]string
		f         func(context.Context, *sql.DB, string)
		execCount int
	}{
		//{
		//	"01",
		//	map[string]map[string]string{
		//		"mysql": {
		//			"": "select id from client as c where id = 1;",
		//		},
		//		"postgres": {
		//			"": "select id from client as c where id = 1;",
		//		},
		//		"mssql2022": {
		//			"": "select id from client as c where id = 1;",
		//		},
		//	},
		//	Query01,
		//	3000,
		//},
		{
			"02",
			map[string]map[string]string{
				"mysql": {
					"": "select id, name from client as c where id = 1;",
				},
				"postgres": {
					"": "select id, name from client as c where id = 1;",
				},
				"mssql2022": {
					"": "select id, name from client as c where id = 1;",
				},
			},
			Query02,
			3000,
		},
		{
			"03",
			map[string]map[string]string{
				"mysql": {
					"": "select min(id) from client as c;",
				},
				"postgres": {
					"": "select min(id) from client as c;",
				},
				"mssql2022": {
					"": "select min(id) from client as c;",
				},
			},
			Query03,
			1000,
		},
		//{
		//	"05",
		//	map[string]map[string]string{
		//		"mysql": {
		//			"": "select count(*) from `order` as o inner join `order_detail` as od on od.order_id = o.id;",
		//		},
		//		"postgres": {
		//			"": "select count(*) from \"order\" as o inner join order_detail as od on od.order_id = o.id;",
		//			//"hashjoin=off": "SET enable_hashjoin = off; select count(*) from \"order\" as o inner join order_detail as od on od.order_id = o.id;",
		//		},
		//		"mssql2022": {
		//			"": "select count(*) from [order] as o inner join order_detail as od on od.order_id = o.id;",
		//			//"loop join": "select count(*) from [order] as o inner loop join order_detail as od on od.order_id = o.id;",
		//		},
		//	},
		//	Query05,
		//	0,
		//},
	}

	result := make(map[string]string, len(data)*len(databases))

	for _, d := range databases {
		db, err = sql.Open(d.driverName, d.dsn)
		if err != nil {
			log.Fatalf("Unable to connect to database(%s): %v", d.connectionName, err)
		}

		db.SetConnMaxLifetime(0)
		db.SetMaxIdleConns(3)
		db.SetMaxOpenConns(3)
		Ping(ctx, db)

		for _, testData := range data {
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
					key := fmt.Sprintf("%s - %s", testData.testName, d.connectionName)
					if queryName != "" {
						key += fmt.Sprintf("(%s)", queryName)
					}
					result[key] = fmt.Sprintf("%s", duration/time.Duration(config.TestExecutions))
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
