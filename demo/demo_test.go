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

var mysql_db *sql.DB    // Database connection pool.
var postgres_db *sql.DB // Database connection pool.
var mssql_db *sql.DB    // Database connection pool.

func Ping(ctx context.Context, db *sql.DB) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
}

func MySql01(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := mysql_db.QueryRowContext(ctx, "select id from client as c where id = 1;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func Postgres01(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := postgres_db.QueryRowContext(ctx, "select id from client as c where id = 1;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func MsSql01(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := mssql_db.QueryRowContext(ctx, "select id from client as c where id = 1;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func MySql02(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var id int
	var name string
	err := mysql_db.QueryRowContext(ctx, "select id, name from client as c where id = 1;").Scan(&id, &name)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func Postgres02(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var id int
	var name string
	err := postgres_db.QueryRowContext(ctx, "select id, name from client as c where id = 1;").Scan(&id, &name)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func MsSql02(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var id int
	var name string
	err := mssql_db.QueryRowContext(ctx, "select id, name from client as c where id = 1;").Scan(&id, &name)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func MySql05(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := mysql_db.QueryRowContext(ctx, "select count(*) from `order` as o inner join `order_detail` as od on od.order_id = o.id;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func Postgres05(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := postgres_db.QueryRowContext(ctx, "SET enable_hashjoin = off; select count(*) from \"order\" as o inner join order_detail as od on od.order_id = o.id;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func MsSql05(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := mssql_db.QueryRowContext(ctx, "select count(*) from [order] as o inner join order_detail as od on od.order_id = o.id;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	//log.Println("result = ", result)
}

func ExecQuery(ctx context.Context, f func(context.Context), execs int) time.Duration {
	for i := 0; i < config.WarmUpExecutions; i++ {
		f(ctx)
	}

	if execs == 0 {
		execs = config.TestExecutions
	}
	start := time.Now()
	for i := 0; i < execs; i++ {
		f(ctx)
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
		name       string
		mysql_f    func(context.Context)
		postgres_f func(context.Context)
		mssql_f    func(context.Context)
		execs      int
	}{
		//{"01", MySql01, Postgres01, MsSql01, 3000},
		{"02", MySql02, Postgres02, MsSql02, 3000},
		//{"q5", MySql05, Postgres05, MsSql05},
	}

	// MySQL
	mysql_db, err = sql.Open("mysql", "root:mysql@tcp(127.0.0.1:3306)/test_db")
	defer mysql_db.Close()
	if err != nil {
		log.Fatal(err)
	}
	mysql_db.SetConnMaxLifetime(0)
	mysql_db.SetMaxIdleConns(3)
	mysql_db.SetMaxOpenConns(3)
	Ping(ctx, mysql_db)

	// PostgreSQL
	postgres_db, err = sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/test_db?sslmode=disable")
	defer postgres_db.Close()
	if err != nil {
		log.Fatal(err)
	}
	postgres_db.SetConnMaxLifetime(0)
	postgres_db.SetMaxIdleConns(3)
	postgres_db.SetMaxOpenConns(3)
	Ping(ctx, postgres_db)

	// MSSQL
	mssql_db, err = sql.Open("sqlserver", "sqlserver://SA:myStrong(!)Password@localhost:1433?database=test_db")
	defer mssql_db.Close()
	if err != nil {
		log.Fatal(err)
	}
	mssql_db.SetConnMaxLifetime(0)
	mssql_db.SetMaxIdleConns(3)
	mssql_db.SetMaxOpenConns(3)
	Ping(ctx, mssql_db)

	result := make(map[string]string, len(data)*3)

	for _, d := range data {
		duration := ExecQuery(ctx, d.mysql_f, d.execs)
		key := fmt.Sprintf("%s - mysql", d.name)
		result[key] = fmt.Sprintf("%s", duration/time.Duration(config.TestExecutions))

		duration = ExecQuery(ctx, d.postgres_f, d.execs)
		key = fmt.Sprintf("%s - postgres", d.name)
		result[key] = fmt.Sprintf("%s", duration/time.Duration(config.TestExecutions))

		duration = ExecQuery(ctx, d.mssql_f, d.execs)
		key = fmt.Sprintf("%s - mssql", d.name)
		result[key] = fmt.Sprintf("%s", duration/time.Duration(config.TestExecutions))
	}

	prettyResult, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Println("error:", err)
	}

	log.Println(string(prettyResult))
}
