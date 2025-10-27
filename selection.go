package dbfailover

import (
	"database/sql"
	"time"
)

type selection struct {
	master          *sql.DB
	slave           *sql.DB
	lastMaster      *sql.DB
	multipleMasters bool
}

func makeSelection(statuses map[*sql.DB]dbStatus, lastMaster *sql.DB) selection {
	var (
		master          *sql.DB
		masterLatency   time.Duration
		slave           *sql.DB
		slaveLatency    time.Duration
		multipleMasters bool
	)

	for db, status := range statuses {
		switch status.role {
		case roleOffline:
			continue
		case roleMaster:
			multipleMasters = multipleMasters || master != nil

			if masterLatency == 0 || status.latency < masterLatency {
				master = db
				masterLatency = status.latency
			}
		case roleSlave:
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
		master:          master,
		slave:           slave,
		lastMaster:      lastMaster,
		multipleMasters: multipleMasters,
	}
}
