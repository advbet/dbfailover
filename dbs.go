// Package dbfailover monitors set of DB servers and provides easy access to
// currently alive server with desired role (master/slave).
package dbfailover

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
)

const readOnlyInterval = 1500 * time.Millisecond

// DBs holds a list of pools of known DB servers and provides easy access for
// getting currently active master or slave DB pool.
type DBs struct {
	active selection
	stop   func()
	config Config
	mu     sync.RWMutex
}

// Config holds configuration for DB pools.
type Config struct {
	SkipSlaveCheck  bool
	SkipGaleraCheck bool
}

type statusUpdate struct {
	db     *sql.DB
	status dbStatus
}

// ErrNoDatabases is returned from New() if empty slice of databases are
// provided. Without any databases to start with we can not guarantee that
// Master() and Slave() methods will never return nil.
var ErrNoDatabases = errors.New("empty database set provided")

// New creates a new instance of database pools checker.
//
// It will block until initial databases state is detected, therefore it is safe
// to immediately query for master and slave pools after this function returns.
//
// If dbs is empty slice it will return ErrNoDatabases error.
func New(dbs []*sql.DB) (*DBs, error) {
	return NewWithConfig(dbs, Config{})
}

// NewWithConfig is same as New but allows passing a configuration struct.
func NewWithConfig(dbs []*sql.DB, cfg Config) (*DBs, error) {
	if len(dbs) == 0 {
		return nil, ErrNoDatabases
	}

	ctx, cancel := context.WithCancel(context.Background())

	state := checkBatch(dbs, cfg.SkipSlaveCheck, cfg.SkipGaleraCheck)
	lastMaster := dbs[0]

	p := &DBs{
		active: makeSelection(state, lastMaster),
		stop:   cancel,
		config: cfg,
	}
	go p.run(ctx, state, lastMaster)

	return p, nil
}

// Master returns a database pool attached to the currently active master
// database instance.
//
// This function will never return nil. If there are no master servers
// available it will return last seen master. It allows this function result to
// be used without additional checks, example: `dbs.Master().Query(...)`.
func (p *DBs) Master() *sql.DB {
	p.mu.RLock()
	active := p.active
	p.mu.RUnlock()

	if active.master != nil {
		return active.master
	}

	return active.lastMaster
}

// Slave returns database pool attached to a server suitable to be used for
// read-only non time sensitive queries. It tries to return slave instance with
// the lowest delay. If no slaves are detected it returns a master DB instance.
//
// This function will never return nil. If there are no servers available it
// will return last seen master. It allows this function result to be used
// without additional checks, example: `dbs.Slave().Query(...)`.
func (p *DBs) Slave() *sql.DB {
	p.mu.RLock()
	active := p.active
	p.mu.RUnlock()

	if active.slave != nil {
		return active.slave
	}

	return active.lastMaster
}

// Stop kills DB status checking go-routines. Functions to get master or slave
// DB pools can be safely used after Stop is called. They will return last seen
// state before Stop was called.
func (p *DBs) Stop() {
	p.stop()
}

func (p *DBs) run(ctx context.Context, state map[*sql.DB]dbStatus, lastMaster *sql.DB) {
	updates := make(chan statusUpdate)
	for db := range state {
		go checkLoop(ctx, db, updates, p.config.SkipSlaveCheck, p.config.SkipGaleraCheck)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case u := <-updates:
			state[u.db] = u.status
			active := makeSelection(state, lastMaster)

			p.mu.Lock()
			p.active = active
			p.mu.Unlock()

			// persist lastMaster pool for next iteration
			lastMaster = active.lastMaster
		}
	}
}

func checkBatch(dbs []*sql.DB, skipSlaveCheck bool, skipWsrepCheck bool) map[*sql.DB]dbStatus {
	ss := make([]dbStatus, len(dbs))
	var wg sync.WaitGroup
	wg.Add(len(dbs))
	for i := range dbs {
		go func(i int) {
			defer wg.Done()
			ss[i] = checkDBStatus(dbs[i], skipSlaveCheck, skipWsrepCheck)
		}(i)
	}
	wg.Wait()

	out := make(map[*sql.DB]dbStatus)
	for i, s := range ss {
		out[dbs[i]] = s
	}
	return out
}

func checkLoop(ctx context.Context, db *sql.DB, updates chan<- statusUpdate, skipSlaveCheck bool, skipWsrepCheck bool) {
	t := time.NewTicker(readOnlyInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			status := checkDBStatus(db, skipSlaveCheck, skipWsrepCheck)
			select {
			case <-ctx.Done():
				return
			case updates <- statusUpdate{db: db, status: status}:
				//OK
			}
		}
	}
}
