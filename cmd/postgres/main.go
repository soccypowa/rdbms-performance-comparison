package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/lib/pq"
)

var db *sql.DB // Database connection pool.

var host = "localhost"
var port = 54320
var user = "postgres"
var password = "postgres"
var dbname = "q1"

func main() {
	// Build connection string
	psqlConnString := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var err error

	db, err = sql.Open("postgres", psqlConnString)
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
		Query1(ctx, 1)
	}

	start := time.Now()

	for i := 0; i < testExecutions; i++ {
		Query1(ctx, 1)
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
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from public.q1 as p where status_id = $1;", sql.Named("status_id", status_id)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query2(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from public.q2 as p where status = $1;", sql.Named("status", status)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query3(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from public.q3 as p where status = $1;", sql.Named("status", status)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func Query4(ctx context.Context, status string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from public.q4 as p where status = $1;", sql.Named("status", status)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}
