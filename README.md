dbfailover
==========

[![GoDoc](https://godoc.org/bitbucket.org/advbet/dbfailover?status.svg)](https://godoc.org/bitbucket.org/advbet/dbfailover)

This is a go package for managing access to multiple MySQL/MariaDB servers. This
package takes a list of DB handlers (as slice of `*sql.DB`) and provides methods
for getting currently active master slave DB instance.

Server role detection
---------------------

Server role (master or slave) is selected by periodically checking value of
`read_only` variable in the server configuration. When `read_only` flag is set
to true server is considered to have a slave role otherwise server role is
master. This is important to take into consideration when performing server
failover. If server is actually running in slave mode but have `read_only` flag
set to false it will receive DML queries from the services using this package
and this will most likely cause data replication failure.

Usage example
-------------

```go
package main

import (
        "database/sql"
        "log"

        "bitbucket.org/advbet/dbfailover"
        _ "github.com/go-sql-driver/mysql"
)

type Service struct {
        dbs *dbfailover.DBs
}

func main() {
        dsns := []string{
                "user:pass@tcp(127.0.0.1:3306)/db",
                "user:pass@tcp(127.0.0.2:3306)/db",
                "user:pass@tcp(127.0.0.3:3306)/db",
        }

        var dbhs []*sql.DB
        for _, dsn := range dsns {
                db, err := sql.Open("mysql", dsn)
                if err != nil {
                        log.Print("failed to create db pool instance", err)
                        continue
                }
                dbhs = append(dbhs, db)
        }

        dbs, err := dbfailover.New(dbhs)
        if err != nil {
                log.Fatal("failed to create DBs", err)
        }
        defer dbs.Stop()

        svc := Service{
                dbs: dbs,
        }

        svc.Insert()
        svc.Query()
}

func (s *Service) Insert() {
        // Access master server
        _, err := s.dbs.Master().Query(`INSERT INTO user(id, name) VALUES(NULL, "John")`)
        if err != nil {
                log.Fatal("insert query on master failed", err)
        }
}

func (s *Service) Query() {
        // Access slave server
        _, err := s.dbs.Slave().Query(`SELECT id, name FROM user`)
        if err != nil {
                log.Fatal("select query on slave failed", err)
        }
}
```
