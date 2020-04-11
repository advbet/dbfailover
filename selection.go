package dbfailover

import (
	"database/sql"
	"time"
)

type selection struct {
	master     *sql.DB
	slave      *sql.DB
	lastMaster *sql.DB
}

func makeSelection(statuses map[*sql.DB]dbStatus, lastMaster *sql.DB) selection {
	var master *sql.DB
	var masterLatency time.Duration
	var slave *sql.DB
	var slaveLatency time.Duration

	for db, status := range statuses {
		switch {
		case !status.online:
			continue
		case !status.readOnly:
			if masterLatency == 0 || status.latency < masterLatency {
				master = db
				masterLatency = status.latency
			}
		case status.readOnly:
			if slaveLatency == 0 || status.latency < slaveLatency {
				slave = db
				slaveLatency = status.latency
			}

		}
	}
	if slave == nil {
		slave = master
	}
	if master != nil {
		lastMaster = master
	}

	return selection{
		master:     master,
		slave:      slave,
		lastMaster: lastMaster,
	}
}
