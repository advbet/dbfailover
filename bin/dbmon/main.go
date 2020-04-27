package main

import (
	"database/sql"
	"flag"
	"log"
	"strings"
	"time"

	"bitbucket.org/advbet/dbfailover"
	"github.com/go-sql-driver/mysql"
)

func main() {
	var dsnsFlag string

	flag.StringVar(&dsnsFlag, "dsns", "", "List of comma separated DB DSN")
	flag.Parse()

	var dbs []*sql.DB
	hosts := make(map[*sql.DB]string)
	for _, dsn := range strings.Split(dsnsFlag, ",") {
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			log.Fatal("invalid DSN (", dsn, "): ", err)
		}
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			log.Fatal("erro opening DSN (", dsn, "): ", err)
		}
		dbs = append(dbs, db)
		hosts[db] = cfg.Addr
	}

	db, err := dbfailover.New(dbs)
	if err != nil {
		log.Fatal("creating dbfailover pool: ", err)
	}

	for {
		master := db.Master()
		slave := db.Slave()
		log.Print("master: ", hosts[master])
		log.Print("slave: ", hosts[slave])
		time.Sleep(time.Second)
	}
}
