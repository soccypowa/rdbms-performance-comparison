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

var mysqlDb *sql.DB    // Database connection pool.
var postgresDb *sql.DB // Database connection pool.
var mssqlDb *sql.DB    // Database connection pool.

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

func MySql05(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := mysqlDb.QueryRowContext(ctx, "select count(*) from `order` as o inner join `order_detail` as od on od.order_id = o.id;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func Postgres05(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := postgresDb.QueryRowContext(ctx, "SET enable_hashjoin = off; select count(*) from \"order\" as o inner join order_detail as od on od.order_id = o.id;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func MsSql05(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := mssqlDb.QueryRowContext(ctx, "select count(*) from [order] as o inner join order_detail as od on od.order_id = o.id;").Scan(&result)
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
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		<-appSignal
		stop()
	}()

	data := []struct {
		name           string
		mysql_query    string
		postgres_query string
		mssql_query    string
		f              func(context.Context, *sql.DB, string)
		execs          int
	}{
		{
			"01",
			"select id from client as c where id = 1;",
			"select id from client as c where id = 1;",
			"select id from client as c where id = 1;",
			Query01,
			3000,
		},
		//{
		//	"02",
		//	"select id, name from client as c where id = 1;",
		//	"select id, name from client as c where id = 1;",
		//	"select id, name from client as c where id = 1;",
		//	Query02,
		//	3000,
		//},
		//{
		//	"03",
		//	"select min(id) from client as c;",
		//	"select min(id) from client as c;",
		//	"select min(id) from client as c;",
		//	Query03,
		//	1000,
		//},
		//{"q5", MySql05, Postgres05, MsSql05},
	}

	// MySQL
	mysqlDb, err = sql.Open("mysql", "root:mysql@tcp(127.0.0.1:3306)/test_db")
	defer mysqlDb.Close()
	if err != nil {
		log.Fatal(err)
	}
	mysqlDb.SetConnMaxLifetime(0)
	mysqlDb.SetMaxIdleConns(3)
	mysqlDb.SetMaxOpenConns(3)
	Ping(ctx, mysqlDb)

	// PostgreSQL
	postgresDb, err = sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/test_db?sslmode=disable")
	defer postgresDb.Close()
	if err != nil {
		log.Fatal(err)
	}
	postgresDb.SetConnMaxLifetime(0)
	postgresDb.SetMaxIdleConns(3)
	postgresDb.SetMaxOpenConns(3)
	Ping(ctx, postgresDb)

	// MSSQL
	mssqlDb, err = sql.Open("sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1433?database=test_db")
	defer mssqlDb.Close()
	if err != nil {
		log.Fatal(err)
	}
	mssqlDb.SetConnMaxLifetime(0)
	mssqlDb.SetMaxIdleConns(3)
	mssqlDb.SetMaxOpenConns(3)
	Ping(ctx, mssqlDb)

	result := make(map[string]string, len(data)*3)

	for _, d := range data {
		duration := ExecQuery(ctx, d.f, mysqlDb, d.mysql_query, d.execs)
		key := fmt.Sprintf("%s - mysql", d.name)
		result[key] = fmt.Sprintf("%s", duration/time.Duration(config.TestExecutions))

		duration = ExecQuery(ctx, d.f, postgresDb, d.postgres_query, d.execs)
		key = fmt.Sprintf("%s - postgres", d.name)
		result[key] = fmt.Sprintf("%s", duration/time.Duration(config.TestExecutions))

		duration = ExecQuery(ctx, d.f, mssqlDb, d.mssql_query, d.execs)
		key = fmt.Sprintf("%s - mssql", d.name)
		result[key] = fmt.Sprintf("%s", duration/time.Duration(config.TestExecutions))
	}

	prettyResult, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Println("error:", err)
	}

	log.Println(string(prettyResult))
}
