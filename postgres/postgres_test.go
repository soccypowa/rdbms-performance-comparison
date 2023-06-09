package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/solontsev/rdbms-performance-comparison/config"

	"github.com/docker/go-connections/nat"
	. "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/lib/pq"
)

var db *sql.DB // Database connection pool.

const dbname = "postgres"
const postgresImage = "postgres:15.3"
const port = "5432/tcp"
const user = "postgres"
const password = "password"

var env = map[string]string{
	"POSTGRES_PASSWORD": password,
	"POSTGRES_USER":     user,
	"POSTGRES_DB":       dbname,
}

var dbURL = func(host string, port nat.Port) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port.Port(), dbname)
}

func startContainer(ctx context.Context, t *testing.T) (Container, string, error) {
	req := ContainerRequest{
		Image:        postgresImage,
		ExposedPorts: []string{port},
		//Cmd:          []string{"postgres", "-c", "fsync=off"},
		Env:        env,
		WaitingFor: wait.ForSQL(nat.Port(port), "postgres", dbURL).WithStartupTimeout(config.ContainerStartupTimeout),
		//WaitingFor: wait.ForSQL(nat.Port(port), "postgres", dbURL).WithStartupTimeout(config.ContainerStartupTimeout).WithQuery("SELECT 10"), // custom query
		Files: []ContainerFile{
			{
				HostFilePath:      "./testdata/q1_init.sql",
				ContainerFilePath: "/docker-entrypoint-initdb.d/q1_init.sql",
				FileMode:          700,
			},
		},
	}
	container, err := GenericContainer(ctx, GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		container.Terminate(ctx)
	})

	mappedPort, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatal(err)
	}

	connectionString := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		"127.0.0.1", mappedPort.Int(), user, password, dbname)

	return container, connectionString, err
}

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
	err := db.QueryRowContext(ctx, "select count(*) from public.test_table as p where status_id = $1;", sql.Named("status_id", status_id)).Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	log.Println("result = ", result)
}

func TestContainerWithWaitForSQL(t *testing.T) {

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		<-appSignal
		stop()
	}()

	t.Run("first test", func(t *testing.T) {
		_, dbConnectionString, err := startContainer(ctx, t)
		if err != nil {
			t.Fatal(err)
		}

		db, err = sql.Open("postgres", dbConnectionString)
		if err != nil {
			log.Fatal("Error creating connection: ", err.Error())
		}
		defer db.Close()

		db.SetConnMaxLifetime(0)
		db.SetMaxIdleConns(3)
		db.SetMaxOpenConns(3)

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
	})
}
