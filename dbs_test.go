package dbfailover

import (
	"database/sql"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	master, cleanup := startMasterInstance(t)
	defer cleanup()
	slave, cleanup := startSlaveInstance(t)
	defer cleanup()
	offline := startOfflineInstance(t)

	p := New([]*sql.DB{master, slave, offline})
	defer p.Stop()

	t.Run("master", func(t *testing.T) {
		m := p.Master()
		if m != master {
			t.Fatalf("master database does not match, got %v, expected %v", m, master)
		}
	})

	t.Run("slave", func(t *testing.T) {
		s := p.Slave()
		if s != slave {
			t.Fatalf("slave database does not match, got %v, expected %v", s, slave)
		}
	})

	// swap roles
	_, err := master.Exec("SET GLOBAL read_only = 1")
	if err != nil {
		t.Fatalf("demoting master to slave: %v", err)
	}
	_, err = slave.Exec("SET GLOBAL read_only = 0")
	if err != nil {
		t.Fatalf("promoting slave to master: %v", err)
	}

	time.Sleep(readOnlyInterval + 100*time.Millisecond)

	t.Run("master as slave", func(t *testing.T) {
		m := p.Slave()
		if m != master {
			t.Fatalf("master database does not match, got %v, expected %v", m, master)
		}
	})

	t.Run("slave as master", func(t *testing.T) {
		s := p.Master()
		if s != slave {
			t.Fatalf("slave database does not match, got %v, expected %v", s, slave)
		}
	})
}
