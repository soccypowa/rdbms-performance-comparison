package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB // Database connection pool.

var host = "localhost"
var port = 3306
var user = "root"
var password = "mysql"
var dbname = "TEST"

func main() {
	// Build connection string
	psqlConnString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		user, password, host, port, dbname)

	var err error

	db, err = sql.Open("mysql", psqlConnString)
	if err != nil {
		log.Fatal("Error creating connection: ", err.Error())
	}
	defer db.Close()

	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(3)
	db.SetMaxOpenConns(3)

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
		Query4(ctx, "active")
	}

	start := time.Now()

	for i := 0; i < testExecutions; i++ {
		Query4(ctx, "active")
	}

	elapsed := time.Since(start)

	log.Printf("Avg execution time: %s", elapsed/time.Duration(testExecutions))
}

// Ping the database to verify DSN provided by the user is valid and the
// server accessible. If the ping fails exit the program with an error.
func Ping(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
}

// Query1 the database for the information requested and prints the results.
// If the query fails exit the program with an error.
func Query1(ctx context.Context, status_id int32) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from q1 as p where status_id = ?;", status_id).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query2(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from q2 as p where status = ?;", status).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query3(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from q3 as p where status = ?;", status).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query4(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from q4 as p where status = ?;", status).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}
