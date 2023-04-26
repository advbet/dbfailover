package dbfailover

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
)

const mariaDBVersion = "10.6"
const mariaDBPassword = ""
const mariaDBUser = "root"
const mariaDBName = "testing"

type voidLogger struct{}

func (l voidLogger) Print(v ...interface{}) {}

func init() {
	_ = mysql.SetLogger(voidLogger{})
}

var poolToHostPort = make(map[*sql.DB]string)

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

func startMariaDB(pool *dockertest.Pool, wsrep bool, peers ...string) (*dockertest.Resource, *sql.DB, error) {
	args := []string{
		"--log-bin",
		"--binlog-format=ROW",
		"--gtid-strict-mode=1",
		//"--wsrep-provider=/usr/lib/libgalera_smm.so",
		"--innodb-autoinc-lock-mode=2",
		fmt.Sprintf("--server-id=%d", rand.Intn(1<<31)),
	}

	if wsrep {
		args = append(args, "--wsrep-on=1")
		args = append(args, fmt.Sprintf("--wsrep-cluster-address=gcomm://%s", strings.Join(peers, ",")))
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mariadb",
		Tag:        mariaDBVersion,
		Cmd:        args,
		Env: []string{
			fmt.Sprintf("MYSQL_DATABASE=%s", mariaDBName),
			"MYSQL_ALLOW_EMPTY_PASSWORD=yes",
			"MYSQL_INITDB_SKIP_TZINFO=yes",
		},
	})
	if err != nil {
		return nil, nil, err
	}
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s",
		mariaDBUser, mariaDBPassword,
		resource.GetHostPort("3306/tcp"), mariaDBName,
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
	poolToHostPort[db] = resource.GetHostPort("3306/tcp")
	return resource, db, nil
}

func startMasterInstance(t *testing.T) (*sql.DB, func()) {
	docker := dockerPool(t)
	r, db, err := startMariaDB(docker, false)
	if err != nil {
		t.Fatalf("starting master DB instance: %v", err)
	}
	return db, func() {
		docker.Purge(r)
	}
}

func startGaleraInstance(t *testing.T, peers ...*sql.DB) (*sql.DB, func()) {
	var hostsPorts []string
	for _, db := range peers {
		hostPort, ok := poolToHostPort[db]
		if !ok {
			t.Fatal("unable to find peer address")
		}
		hostsPorts = append(hostsPorts, hostPort)
	}

	docker := dockerPool(t)
	r, db, err := startMariaDB(docker, true, hostsPorts...)
	if err != nil {
		t.Fatalf("starting galera DB instance: %v", err)
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

func waitForSlaveRunning(db *sql.DB, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for ctx.Err() == nil {
		var key string
		var val string
		err := db.QueryRowContext(ctx, "SHOW STATUS LIKE 'Slave_running'").Scan(&key, &val)
		if err == nil && val == "ON" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ctx.Err()
}

func makeSlaveOf(slave *sql.DB, master *sql.DB) error {
	hostPort, ok := poolToHostPort[master]
	if !ok {
		return fmt.Errorf("unable to find master hostPort address")
	}

	parts := strings.Split(hostPort, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid hostPort address: %s", hostPort)
	}

	var key, binLogPos string
	if err := master.QueryRow("show variables like 'gtid_binlog_pos'").Scan(&key, &binLogPos); err != nil {
		return fmt.Errorf("checking binlog pos on master: %w", err)
	}

	_, err := slave.Exec(fmt.Sprintf("SET GLOBAL gtid_slave_pos = '%s'", binLogPos))
	if err != nil {
		return fmt.Errorf("updating expected slave start pos: %w", err)
	}
	_, err = slave.Exec(fmt.Sprintf("CHANGE MASTER TO MASTER_HOST = '%s', MASTER_PORT = %s, MASTER_USER = '%s', MASTER_PASSWORD = '%s', MASTER_USE_GTID = slave_pos", getHost(), parts[1], mariaDBUser, mariaDBPassword))
	if err != nil {
		return fmt.Errorf("configuring master connection on slave server: %w", err)
	}
	_, err = slave.Exec("START SLAVE")
	if err != nil {
		return fmt.Errorf("starting slave process on slave server: %w", err)
	}
	return nil
}

func startSlaveInstance(t *testing.T, master *sql.DB) (*sql.DB, func()) {
	docker := dockerPool(t)
	r, db, err := startMariaDB(docker, false)
	if err != nil {
		t.Fatalf("starting slave DB instance: %v", err)
	}

	_, err = db.Exec("SET GLOBAL read_only = 1")
	if err != nil {
		docker.Purge(r)
		t.Fatalf("setting read_only flag on slave server: %v", err)
	}

	if master != nil {
		if err := makeSlaveOf(db, master); err != nil {
			docker.Purge(r)
			t.Fatalf("failed to configure slave server: %v", err)
		}
		if err := waitForSlaveRunning(db, 5*time.Second); err != nil {
			docker.Purge(r)
			t.Fatalf("waiting for slave connection to be esablished: %v", err)
		}
	}

	return db, func() {
		docker.Purge(r)
	}
}

func getHost() string {
	containersURL := os.Getenv("CONTAINERS_HOST")
	if containersURL != "" {
		return containersURL
	}

	return dockerHost
}
