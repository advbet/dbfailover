package dbfailover

import (
	"context"
	"database/sql"
	"time"
)

type dbStatus struct {
	online   bool
	readOnly bool
	latency  time.Duration
}

const queryTimeout = 1 * time.Second

func checkDBStatus(db *sql.DB) dbStatus {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	var key string
	var val string
	start := time.Now()
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'read_only'").Scan(&key, &val)
	d := time.Since(start)

	if err != nil {
		return dbStatus{
			online:  false,
			latency: d,
		}
	}
	return dbStatus{
		online:   true,
		readOnly: val == "ON",
		latency:  d,
	}
}
