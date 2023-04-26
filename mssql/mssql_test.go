package mssql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/solontsev/rdbms-performance-comparison/config"

	"github.com/docker/go-connections/nat"
	. "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/microsoft/go-mssqldb"
)

var db *sql.DB // Database connection pool.

const dbname = "tempdb"
const dockerImage = "mcr.microsoft.com/mssql/server:2019-CU20-ubuntu-20.04"
const port = "1433/tcp"
const user = "SA"
const password = "myStrong(!)Password"

var env = map[string]string{
	"ACCEPT_EULA":       "Y",
	"MSSQL_USER":        user,
	"MSSQL_SA_PASSWORD": password,
	"MSSQL_PID":         "Developer",
}

var dbURL = func(host string, port nat.Port) string {
	return fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s", user, password, host, port.Port(), dbname)
}

func StreamToString(stream io.Reader) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.String()
}

func startContainer(ctx context.Context, t *testing.T) (Container, string, error) {
	req := ContainerRequest{
		Image:        dockerImage,
		ExposedPorts: []string{port},
		Env:          env,
		WaitingFor:   wait.ForSQL(nat.Port(port), "sqlserver", dbURL).WithStartupTimeout(config.ContainerStartupTimeout),
		//WaitingFor: wait.ForSQL(nat.Port(port), "postgres", dbURL).WithStartupTimeout(config.ContainerStartupTimeout).WithQuery("SELECT 10"), // custom query
		Files: []ContainerFile{
			{
				HostFilePath:      "./testdata/q1_init.sql",
				ContainerFilePath: "/tmp/q1_init.sql",
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

	var result int
	var reader io.Reader
	result, reader, err = container.Exec(ctx, []string{
		"/opt/mssql-tools/bin/sqlcmd", "-S", "localhost", "-U", user, "-P", password, "-d", "master", "-i", "/tmp/q1_init.sql",
	})
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("Result = %d", result)
	fmt.Println(StreamToString(reader))

	mappedPort, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatal(err)
	}

	connectionString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		"127.0.0.1", user, password, mappedPort.Int(), "q1")

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
	err := db.QueryRowContext(ctx, "select count(*) from dbo.q1 as p where status_id = @status_id;", sql.Named("status_id", status_id)).Scan(&result)
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

		db, err = sql.Open("sqlserver", dbConnectionString)
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
