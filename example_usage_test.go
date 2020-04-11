package dbfailover_test

import (
	"database/sql"
	"log"

	"bitbucket.org/advbet/dbfailover"
	_ "github.com/go-sql-driver/mysql"
)

type Service struct {
	dbs *dbfailover.DBs
}

func Example() {
	dsns := []string{
		"user:pass@tcp(127.0.0.1:3306)/db",
		"user:pass@tcp(127.0.0.2:3306)/db",
		"user:pass@tcp(127.0.0.3:3306)/db",
	}

	var dbs []*sql.DB
	for _, dsn := range dsns {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Print("failed to create db pool instance", err)
			continue
		}
		dbs = append(dbs, db)
	}

	pools := dbfailover.New(dbs)
	defer pools.Stop()

	svc := Service{
		dbs: pools,
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
