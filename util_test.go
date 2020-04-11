package dbfailover

import (
	"database/sql"
	"fmt"
	"net"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest"
)

const mariaDBVersion = "10.3"
const mariaDBPassword = "secret"
const mariaDBUser = "root"
const mariaDBName = "testing"

type voidLogger struct{}

func (l voidLogger) Print(v ...interface{}) {}

func init() {
	mysql.SetLogger(voidLogger{})
}

var dockerPool = func() func(t *testing.T) *dockertest.Pool {
	var err error
	var pool *dockertest.Pool
	return func(t *testing.T) *dockertest.Pool {
		if pool == nil {
			pool, err = dockertest.NewPool("")
		}
		if err != nil {
			t.Fatalf("creating dockertest pool instance: %v", err)
		}
		return pool
	}
}()

func startMariaDB(pool *dockertest.Pool) (*dockertest.Resource, *sql.DB, error) {
	resource, err := pool.Run("mariadb", mariaDBVersion, []string{
		fmt.Sprintf("MYSQL_ROOT_PASSWORD=%s", mariaDBPassword),
		fmt.Sprintf("MYSQL_DATABASE=%s", mariaDBName),
	})
	if err != nil {
		return nil, nil, err
	}
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:3306)/%s",
		mariaDBUser, mariaDBPassword,
		resource.Container.NetworkSettings.IPAddress, mariaDBName,
	)
	if err = pool.Retry(func() error {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return err
		}
		defer db.Close()
		return db.Ping()
	}); err != nil {
		pool.Purge(resource)
		return nil, nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		pool.Purge(resource)
		return nil, nil, err
	}
	return resource, db, nil
}

func startMasterInstance(t *testing.T) (*sql.DB, func()) {
	docker := dockerPool(t)
	r, db, err := startMariaDB(docker)
	if err != nil {
		t.Fatalf("starting master DB instance: %v", err)
	}
	return db, func() {
		docker.Purge(r)
	}
}

func startOfflineInstance(t *testing.T) *sql.DB {
	sock, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to open new socket: %v", err)
	}
	addr := sock.Addr().String()
	_ = sock.Close()
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s",
		mariaDBUser, mariaDBPassword, addr, mariaDBName,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to create DB instance: %v", err)
	}
	return db
}

func startSlaveInstance(t *testing.T) (*sql.DB, func()) {
	docker := dockerPool(t)
	r, db, err := startMariaDB(docker)
	if err != nil {
		t.Fatalf("starting slave DB instance: %v", err)
	}

	_, err = db.Exec("SET GLOBAL read_only = 1")
	if err != nil {
		docker.Purge(r)
		t.Fatalf("setting read_only flag on slave server: %v", err)
	}

	return db, func() {
		docker.Purge(r)
	}
}
