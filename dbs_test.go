package dbfailover

import (
	"database/sql"
	"testing"
	"time"
)

func TestNewEmpty(t *testing.T) {
	_, err := New(nil)
	if err != ErrNoDatabases {
		t.Fatalf("creating DBs with empty slice does not return ErrNoDatabases")
	}
}

func TestFailover(t *testing.T) {
	pool := getDockerPool(t)
	network := getDockerNetwork(t, pool)
	defer func() {
		pool.RemoveNetwork(network)
	}()

	adb, masterResource := startMasterInstance(t, pool, network)
	defer func() {
		pool.Purge(masterResource)
	}()

	bdb, slaveResource := startSlaveInstance(t, pool, network, adb)
	defer func() {
		pool.Purge(slaveResource)
	}()

	cdb := startOfflineInstance(t)

	names := map[*sql.DB]string{
		adb: "A",
		bdb: "B",
		cdb: "C",
	}

	p, err := New([]*sql.DB{adb, bdb, cdb})
	if err != nil {
		t.Fatalf("creating DBs failed with: %v", err)
	}
	defer p.Stop()

	t.Run("initial setup", func(t *testing.T) {
		m := p.Master()
		if m != adb {
			t.Fatalf("master database does not match, got %v, expected %v", names[m], names[adb])
		}
		s := p.Slave()
		if s != bdb {
			t.Fatalf("slave database does not match, got %v, expected %v", names[s], names[bdb])
		}
	})

	// perform failover
	// step 1 make B db writable
	_, err = bdb.Exec("SET GLOBAL read_only = 0")
	if err != nil {
		t.Fatalf("making B db writable: %v", err)
	}
	time.Sleep(defaultCheckInterval + 100*time.Millisecond)
	t.Run("step 1 B is writable", func(t *testing.T) {
		m := p.Master()
		if m != adb {
			t.Fatalf("master database does not match, got %v, expected %v", names[m], names[adb])
		}
		s := p.Slave()
		if s != bdb {
			t.Fatalf("slave database does not match, got %v, expected %v", names[s], names[bdb])
		}
	})

	// step 2 make A db read-only
	_, err = adb.Exec("SET GLOBAL read_only = 1")
	if err != nil {
		t.Fatalf("making A db read-only: %v", err)
	}
	time.Sleep(defaultCheckInterval + 100*time.Millisecond)
	t.Run("step 2 A is read-only", func(t *testing.T) {
		// master is considered offline now, we are getting old master as fallback
		m := p.Master()
		if m != adb {
			t.Fatalf("master database does not match, got %v, expected %v", names[m], names[adb])
		}
		s := p.Slave()
		if s != bdb {
			t.Fatalf("slave database does not match, got %v, expected %v", names[s], names[bdb])
		}
	})

	// step 3 configure A db as slave
	err = makeSlaveOf(adb, bdb)
	if err != nil {
		t.Fatalf("making B db slave of A db: %v", err)
	}
	time.Sleep(defaultCheckInterval + 100*time.Millisecond)
	t.Run("step 3 A is slave of B", func(t *testing.T) {
		// master is considered slave now, we are getting old master as fallback
		m := p.Master()
		if m != adb {
			t.Fatalf("master database does not match, got %v, expected %v", names[m], names[adb])
		}
		// both servers are in slave role now
		s := p.Slave()
		if s != bdb && s != adb {
			t.Fatalf("slave database does not match, got %v, expected %v or %v", names[s], names[bdb], names[adb])
		}
	})

	// step 4 reset slave on B db
	_, err = bdb.Exec("STOP SLAVE")
	if err != nil {
		t.Fatalf("stopping slave on B db: %v", err)
	}
	_, err = bdb.Exec("RESET SLAVE")
	if err != nil {
		t.Fatalf("resetting slave on B db: %v", err)
	}
	time.Sleep(defaultCheckInterval + 100*time.Millisecond)
	t.Run("step 4 B is master", func(t *testing.T) {
		m := p.Master()
		if m != bdb {
			t.Fatalf("master database does not match, got %v, expected %v", names[m], names[bdb])
		}
		s := p.Slave()
		if s != adb {
			t.Fatalf("slave database does not match, got %v, expected %v", names[s], names[adb])
		}
	})
}
