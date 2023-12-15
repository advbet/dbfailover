package v3

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var (
	defaultRefreshTimeout  = 7 * time.Second
	defaultRefreshInterval = 30 * time.Second
)

var ErrTimeout = errors.New("timeout")

type FailoverStatus string

const (
	FailoverStatusHealthy            FailoverStatus = "healthy"
	FailoverStatusMissingMaster      FailoverStatus = "missing_master"
	FailoverStatusConflictingMasters FailoverStatus = "conflicting_masters"
	FailoverStatusUnresponsiveSlaves FailoverStatus = "unresponsive_slaves"
)

type dbState int

const (
	dbStateUnavailable dbState = iota
	dbStateMaster
	dbStateSlave
)

type failoverDB struct {
	*sql.DB

	state    dbState
	errLogFn func(error)
}

// FailoverOption is a failover modification option.
type FailoverOption func(f *Failover)

func WithRefreshInterval(intv time.Duration) FailoverOption {
	return FailoverOption(func(f *Failover) {
		f.refreshInterval = intv
	})
}

func WithRefreshTimeout(intv time.Duration) FailoverOption {
	return FailoverOption(func(f *Failover) {
		f.refreshTimeout = intv
	})
}

func WithStatusUpdateFn(fn func(FailoverStatus)) FailoverOption {
	return FailoverOption(func(f *Failover) {
		f.statusUpdateFn = fn
	})
}

func WithErrorLogFn(fn func(error)) FailoverOption {
	return FailoverOption(func(f *Failover) {
		f.errorLogFn = fn
	})
}

// Failover represents a failover database client.
type Failover struct {
	mu sync.RWMutex

	dbs []*failoverDB

	status FailoverStatus

	master *failoverDB
	slaves []*failoverDB

	refreshInterval time.Duration
	refreshTimeout  time.Duration

	recoverSlavesWithMaster bool

	statusUpdateFn func(FailoverStatus)
	errorLogFn     func(error)
}

// New creates a new failover instance.
// An error is returned if the failover is found to be in an invalid state.
func New(ctx context.Context, dbs []*sql.DB, opts ...FailoverOption) (*Failover, error) {
	if len(dbs) == 0 {
		return nil, errors.New("no databases provided")
	}

	failover := &Failover{
		dbs:             make([]*failoverDB, len(dbs)),
		refreshTimeout:  defaultRefreshTimeout,
		refreshInterval: defaultRefreshInterval,
		errorLogFn:      func(error) {},
		statusUpdateFn:  func(FailoverStatus) {},
	}

	for i := range dbs {
		failover.dbs[i] = &failoverDB{
			DB:       dbs[i],
			errLogFn: failover.errorLogFn,
		}
	}

	for _, opt := range opts {
		opt(failover)
	}

	failover.updateFailover(ctx)

	if failover.status != FailoverStatusHealthy {
		return nil, fmt.Errorf("initializing failover resulted in invalid status %s", failover.status)
	}

	return failover, nil
}

// Run runs the failover client.
func (f *Failover) Run(ctx context.Context) {
	ticker := time.NewTicker(f.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.updateFailover(ctx)
		}
	}
}

// Master returns master sql.DB instance.
func (f *Failover) Master() *sql.DB {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.status != FailoverStatusHealthy {
		f.errorLogFn(fmt.Errorf("retrieving master in invalid failover state %s", f.status))
	}

	return f.master.DB
}

// Slave returns a random Slave instance.
func (f *Failover) Slave() *sql.DB {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch f.status {
	case FailoverStatusHealthy, FailoverStatusUnresponsiveSlaves:
		// OK
	default:
		f.errorLogFn(fmt.Errorf("retrieving slave in invalid failover state %s", f.status))
	}

	if len(f.slaves) > 0 {
		return f.slaves[rand.Intn(len(f.slaves))].DB
	}

	// This only happens if no slaves were in the topology
	return f.master.DB
}

func (f *Failover) updateFailover(ctx context.Context) {
	f.mu.Lock()

	// refresh individual database statuses
	f.refreshDBs(ctx)

	// determine failover status by inspecting
	// individual database states
	f.updateStatus()

	// notify status update
	f.statusUpdateFn(f.status)

	f.mu.Unlock()
}

func (f *Failover) refreshDBs(ctx context.Context) {
	var wg sync.WaitGroup

	for _, db := range f.dbs {
		db := db

		go func() {
			refreshCtx, cancel := context.WithTimeout(ctx, f.refreshTimeout)
			defer cancel()

			db.refresh(refreshCtx)
			wg.Done()
		}()
	}

	wg.Wait()
}

func (f *Failover) updateStatus() {
	var (
		master      *failoverDB
		masterCount uint
		slaves      []*failoverDB
	)

	for _, db := range f.dbs {
		if db.state == dbStateMaster {
			masterCount++
			master = db

			continue
		}

		if db.state == dbStateSlave {
			slaves = append(slaves, db)
		}
	}

	if masterCount > 1 {
		f.status = FailoverStatusConflictingMasters
		return
	}

	if masterCount == 0 {
		f.status = FailoverStatusMissingMaster
		return
	}

	if len(slaves) == 0 && len(f.dbs) > 1 {
		f.status = FailoverStatusUnresponsiveSlaves
		return
	}

	f.master = master
	f.slaves = slaves

	return
}

func (fdb *failoverDB) refresh(ctx context.Context) {
	var (
		key string
		val string
	)

	err := fdb.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'read_only'").Scan(&key, &val)
	switch {
	case err == nil:
		if strings.ToLower(val) == "on" {
			fdb.state = dbStateSlave
		} else {
			fdb.state = dbStateMaster
		}
	case errors.Is(err, ctx.Err()):
		fdb.state = dbStateUnavailable
	default:
		fdb.state = dbStateUnavailable
		fdb.errLogFn(err)
	}
}
