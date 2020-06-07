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
	var cfg dbfailover.Config

	flag.StringVar(&dsnsFlag, "dsns", "", "List of comma separated DB DSN")
	flag.BoolVar(&cfg.SkipSlaveCheck, "skip-slave-check", false, "Skip slave status checks")
	flag.BoolVar(&cfg.SkipGaleraCheck, "skip-galera-check", false, "Skip galera status checks")
	flag.DurationVar(&cfg.CheckInterval, "check-interval", 1500*time.Millisecond, "Interval between status checks")
	flag.DurationVar(&cfg.CheckTimeout, "check-timeout", 1500*time.Millisecond, "Max check duration before timeout")
	flag.DurationVar(&cfg.CheckTimeout, "max-replication-delay", 5*time.Minute, "Max allowed slave delay behind master")
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

	db, err := dbfailover.NewWithConfig(dbs, cfg)
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
