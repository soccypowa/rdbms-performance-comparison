package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/solontsev/rdbms-performance-comparison/config"

	. "github.com/testcontainers/testcontainers-go"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB // Database connection pool.
var container Container

// const defaultDbName = "master"
const testDbName = "test_db"
const port = 3306
const user = "root"
const password = "mysql"
const startContainer = false

var dockerImages = []string{
	"mysql:8.0.33",
}

func Ping(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
}

func ExecQuery(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var result int32
	err := db.QueryRowContext(ctx, "select count(*) from `order` as o inner join `order_detail` as od on od.order_id = o.id;").Scan(&result)
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	//log.Println("result = ", result)
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

	data := []struct {
		name       string
		initScript string
		f          func(context.Context)
	}{
		{"q5", "empty.sql", ExecQuery},
	}

	result := make(map[string]string, len(data))

	for _, dockerImage := range dockerImages {
		var err error
		//var dbConnectionString string
		//container, dbConnectionString, err = startContainer(ctx, dockerImage, t)
		//if err != nil {
		//	t.Fatal(err)
		//}

		dbConnectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, "localhost", port, testDbName)

		db, err = sql.Open("mysql", dbConnectionString)
		if err != nil {
			log.Fatal("Error creating connection: ", err.Error())
		}

		db.SetConnMaxLifetime(0)
		db.SetMaxIdleConns(3)
		db.SetMaxOpenConns(3)

		Ping(ctx)

		for _, d := range data {
			//initScriptContainerPath := fmt.Sprintf("/tmp/%s", d.initScript)
			//execResult, reader, err := container.Exec(ctx, []string{
			//	"/opt/mssql-tools/bin/sqlcmd", "-S", "localhost", "-U", user, "-P", password, "-d", "master", "-i", initScriptContainerPath,
			//})
			//if err != nil {
			//	t.Fatal(err)
			//}
			//log.Printf("Init script(%s) result = %d, output:\n%s\n", initScriptContainerPath, execResult, StreamToString(reader))

			t.Run(d.name, func(t *testing.T) {
				log.Printf("Starting test %s on image %s...", d.name, dockerImage)

				for i := 0; i < config.WarmUpExecutions; i++ {
					d.f(ctx)
				}

				start := time.Now()

				for i := 0; i < config.TestExecutions; i++ {
					d.f(ctx)
				}

				elapsed := time.Since(start)

				key := fmt.Sprintf("%s - %s", dockerImage, d.name)
				result[key] = fmt.Sprintf("%s", elapsed/time.Duration(config.TestExecutions))
			})
		}

		db.Close()
	}

	prettyResult, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Println("error:", err)
	}

	log.Println(string(prettyResult))
}
