package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

var pool *sql.DB // Database connection pool.

var server = "localhost"
var port = 1433
var user = "SA"
var password = "myStrong(!)Password"
var database = "test"

func main() {
	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)

	var err error

	// Opening a driver typically will not attempt to connect to the database.
	pool, err = sql.Open("sqlserver", connString)
	if err != nil {
		// This will not be a connection error, but a connection string parse error
		// or another initialization error.
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	defer pool.Close()

	pool.SetConnMaxLifetime(0)
	pool.SetMaxIdleConns(3)
	pool.SetMaxOpenConns(3)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		<-appSignal
		stop()
	}()

	warmUpExecutions := 5
	testExecutions := 20

	Ping(ctx)

	for i := 0; i < warmUpExecutions; i++ {
		Query3(ctx, "active")
	}

	start := time.Now()

	for i := 0; i < testExecutions; i++ {
		Query3(ctx, "active")
	}

	elapsed := time.Since(start)

	log.Printf("Avg execution time: %s", elapsed/time.Duration(testExecutions))
}

// Ping the database to verify DSN provided by the user is valid and the
// server accessible. If the ping fails exit the program with an error.
func Ping(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := pool.PingContext(ctx); err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
}

// Query1 the database for the information requested and prints the results.
// If the query fails exit the program with an error.
func Query1(ctx context.Context, status_id int32) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int32
	err := pool.QueryRowContext(ctx, "select count(*) from dbo.q1 as p where status_id = @status_id;", sql.Named("status_id", status_id)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query2(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int32
	err := pool.QueryRowContext(ctx, "select count(*) from dbo.q2 as p where status = @status;", sql.Named("status", status)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query3(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int32
	err := pool.QueryRowContext(ctx, "select count(*) from dbo.q3 as p where status = @status;", sql.Named("status", status)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query4(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int32
	err := pool.QueryRowContext(ctx, "select count(*) from dbo.q4 as p where status = @status;", sql.Named("status", status)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}
