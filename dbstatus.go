package dbfailover

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"time"
)

type role int

const (
	roleOffline role = iota
	roleSlave
	roleMaster
)

type dbStatus struct {
	role    role
	latency time.Duration
}

type readOnlyStatus struct {
	online   bool
	readOnly bool
	latency  time.Duration
}

type slaveStatus struct {
	online     bool
	configured bool
	runningIO  bool
	runningSQL bool
	delay      time.Duration
	latency    time.Duration
}

const queryTimeout = 1 * time.Second
const replicationTimeout = 5 * time.Minute

func maxTime(ts ...time.Duration) time.Duration {
	var max time.Duration
	for _, t := range ts {
		if t > max {
			max = t
		}
	}
	return max
}

func mergeStatus(ss slaveStatus, rs readOnlyStatus) dbStatus {
	role := roleOffline

	switch {
	case !rs.online:
		// skip checking if any of the checks failed
		role = roleOffline
	case rs.readOnly && !ss.online:
		// slave status might fail beacause of missing REPLICTION CLIENT
		// permission, server is read-only.
		role = roleSlave
	case !rs.readOnly && !ss.online:
		// slave status might fail beacause of missing REPLICTION CLIENT
		// permission, server is writable.
		role = roleMaster
	case rs.readOnly && ss.configured && ss.runningIO && ss.runningSQL:
		// Perfect slave, read-only and all slave threads running
		role = roleSlave
	case rs.readOnly && ss.configured && ss.runningIO && !ss.runningSQL:
		// Slave is configured but replication have stopped
		// replication delay measuremet is not available
		role = roleOffline
	case rs.readOnly && ss.configured && !ss.runningIO:
		// Slave is configured but not started or stopped already
		role = roleOffline
	case rs.readOnly && !ss.configured:
		// Server is read-only without slave replication configuration,
		// might be miss-configuration or master is being demoted to
		// slave.
		role = roleOffline
	case !rs.readOnly && ss.configured && ss.runningIO && ss.runningSQL:
		// Fully working slave but without read-only flag. Dangerous but
		// valid configuration.
		role = roleSlave
	case !rs.readOnly && ss.configured && ss.runningIO && !ss.runningSQL:
		// Faulty slave and without read-only flag. Extremely dangerous
		// tread as offline.
		role = roleOffline
	case !rs.readOnly && ss.configured && !ss.runningIO:
		// No read-only flag, slave is configured but not running, most
		// likely old slave newly promoted to master. This happens
		// after SLAVE RESET.
		role = roleMaster
	case !rs.readOnly && !ss.configured:
		// Perfect master, not read-only, no slave configuration
		role = roleMaster
	}

	// Make sure slave server is not lagging behind
	if role == roleSlave && ss.delay > replicationTimeout {
		role = roleOffline
	}

	return dbStatus{
		role:    role,
		latency: maxTime(rs.latency, ss.latency),
	}
}

func checkDBStatus(db *sql.DB) dbStatus {
	var wg sync.WaitGroup
	var ss slaveStatus
	var rs readOnlyStatus
	wg.Add(2)
	go func() {
		defer wg.Done()
		rs = checkReadOnlyStatus(db)
	}()
	go func() {
		defer wg.Done()
		ss = checkSlaveStatus(db)
	}()
	wg.Wait()

	return mergeStatus(ss, rs)
}

func checkReadOnlyStatus(db *sql.DB) readOnlyStatus {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	var key string
	var val string
	start := time.Now()
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'read_only'").Scan(&key, &val)
	d := time.Since(start)

	if err != nil {
		return readOnlyStatus{
			online:  false,
			latency: d,
		}
	}
	return readOnlyStatus{
		online:   true,
		readOnly: val == "ON",
		latency:  d,
	}
}

func checkSlaveStatus(db *sql.DB) slaveStatus {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	start := time.Now()
	rows, err := db.QueryContext(ctx, "SHOW SLAVE STATUS")
	d := time.Since(start)
	if err != nil {
		return slaveStatus{online: false, latency: d}
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return slaveStatus{online: false, latency: d}
	}

	if !rows.Next() {
		// Empty response, server is not a slave
		return slaveStatus{
			online:  true,
			latency: d,
		}
	}

	strs := make([]sql.NullString, len(cols))
	strps := make([]interface{}, len(cols))
	for i := range strs {
		strps[i] = &strs[i]
	}
	if err := rows.Scan(strps...); err != nil {
		return slaveStatus{online: false, latency: d}
	}
	if err := rows.Err(); err != nil {
		return slaveStatus{online: false, latency: d}
	}

	vals := make(map[string]string)
	for i := range cols {
		vals[cols[i]] = strs[i].String
	}

	delay := 7 * 24 * time.Hour
	if val := vals["Seconds_Behind_Master"]; val != "" {
		sec, err := strconv.Atoi(val)
		if err != nil {
			return slaveStatus{online: false, latency: d}
		}
		delay = time.Duration(sec) * time.Second
	}

	return slaveStatus{
		online:     true,
		configured: true,
		runningIO:  vals["Slave_IO_Running"] == "Yes",
		runningSQL: vals["Slave_SQL_Running"] == "Yes",
		delay:      delay,
		latency:    d,
	}
}
