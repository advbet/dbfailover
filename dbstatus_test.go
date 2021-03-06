package dbfailover

import (
	"database/sql"
	"testing"
	"time"
)

func TestMergeStatus(t *testing.T) {
	tests := []struct {
		msg  string
		rs   readOnlyStatus
		ss   slaveStatus
		ws   wsrepStatus
		want dbStatus
	}{
		{
			msg: "read-only check failed",
			rs: readOnlyStatus{
				online: false,
			},
			ss: slaveStatus{
				online: true,
			},
			want: dbStatus{
				role: roleOffline,
			},
		},
		{
			msg: "slave check failed, writable server",
			rs: readOnlyStatus{
				online:   true,
				readOnly: false,
			},
			ss: slaveStatus{
				online: false,
			},
			want: dbStatus{
				role: roleMaster,
			},
		},
		{
			msg: "slave check failed, read-only server",
			rs: readOnlyStatus{
				online:   true,
				readOnly: true,
			},
			ss: slaveStatus{
				online: false,
			},
			want: dbStatus{
				role: roleSlave,
			},
		},
		{
			msg: "perfect master",
			rs: readOnlyStatus{
				online:   true,
				readOnly: false,
			},
			ss: slaveStatus{
				online:     true,
				configured: false,
			},
			want: dbStatus{
				role: roleMaster,
			},
		},
		{
			msg: "perfect master, failed wsrep",
			rs: readOnlyStatus{
				online:   true,
				readOnly: false,
			},
			ss: slaveStatus{
				online:     true,
				configured: false,
			},
			ws: wsrepStatus{
				online: true,
				ready:  false,
			},
			want: dbStatus{
				role: roleOffline,
			},
		},
		{
			msg: "perfect master, good wsrep",
			rs: readOnlyStatus{
				online:   true,
				readOnly: false,
			},
			ss: slaveStatus{
				online:     true,
				configured: false,
			},
			ws: wsrepStatus{
				online: true,
				ready:  true,
			},
			want: dbStatus{
				role: roleMaster,
			},
		},
		{
			msg: "master with old slave config",
			rs: readOnlyStatus{
				online:   true,
				readOnly: false,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  false,
				runningSQL: false,
			},
			want: dbStatus{
				role: roleMaster,
			},
		},
		{
			msg: "failed slave, without read-only",
			rs: readOnlyStatus{
				online:   true,
				readOnly: false,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  true,
				runningSQL: false,
			},
			want: dbStatus{
				role: roleOffline,
			},
		},
		{
			msg: "writable slave",
			rs: readOnlyStatus{
				online:   true,
				readOnly: false,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  true,
				runningSQL: true,
			},
			want: dbStatus{
				role: roleSlave,
			},
		},
		{
			msg: "perfect slave",
			rs: readOnlyStatus{
				online:   true,
				readOnly: true,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  true,
				runningSQL: true,
			},
			want: dbStatus{
				role: roleSlave,
			},
		},
		{
			msg: "perfect slave, failed wsrep",
			rs: readOnlyStatus{
				online:   true,
				readOnly: true,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  true,
				runningSQL: true,
			},
			ws: wsrepStatus{
				online: true,
				ready:  false,
			},
			want: dbStatus{
				role: roleOffline,
			},
		},
		{
			msg: "perfect slave, good wsrep",
			rs: readOnlyStatus{
				online:   true,
				readOnly: true,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  true,
				runningSQL: true,
			},
			ws: wsrepStatus{
				online: true,
				ready:  true,
			},
			want: dbStatus{
				role: roleSlave,
			},
		},
		{
			msg: "perfect slave, but too high replication delay",
			rs: readOnlyStatus{
				online:   true,
				readOnly: true,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  true,
				runningSQL: true,
				delay:      time.Hour,
			},
			want: dbStatus{
				role: roleOffline,
			},
		},
		{
			msg: "failed slave",
			rs: readOnlyStatus{
				online:   true,
				readOnly: true,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  true,
				runningSQL: false,
			},
			want: dbStatus{
				role: roleOffline,
			},
		},
		{
			msg: "stopped slave",
			rs: readOnlyStatus{
				online:   true,
				readOnly: true,
			},
			ss: slaveStatus{
				online:     true,
				configured: true,
				runningIO:  false,
				runningSQL: false,
			},
			want: dbStatus{
				role: roleOffline,
			},
		},
		{
			msg: "read-only server without slave config",
			rs: readOnlyStatus{
				online:   true,
				readOnly: true,
			},
			ss: slaveStatus{
				online:     true,
				configured: false,
			},
			want: dbStatus{
				role: roleOffline,
			},
		},
		{
			msg: "max latency",
			rs: readOnlyStatus{
				online:  true,
				latency: 1 * time.Second,
			},
			ss: slaveStatus{
				online:  true,
				latency: 2 * time.Second,
			},
			want: dbStatus{
				role:    roleMaster,
				latency: 2 * time.Second,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.msg, func(t *testing.T) {
			got := mergeStatus(test.ss, test.rs, test.ws, defaultMaxReplicationDelay)
			if got != test.want {
				t.Errorf("rs: %v, ss: %v, expected: %v, got: %v", test.rs, test.ss, test.want, got)
			}
		})
	}
}

func TestSlaveStatus(t *testing.T) {
	offline := startOfflineInstance(t)
	master, cleanup := startMasterInstance(t)
	defer cleanup()

	stoppedSlave, cleanup := startSlaveInstance(t, master)
	defer cleanup()
	if _, err := stoppedSlave.Exec("STOP SLAVE"); err != nil {
		t.Fatalf("failed to prepare stopped slave: %v", err)
	}

	failedSlave, cleanup := startSlaveInstance(t, master)
	defer cleanup()
	if _, err := failedSlave.Exec("CREATE USER a@localhost"); err != nil {
		t.Fatalf("executing DML on slave to fail replication: %v", err)
	}
	if _, err := master.Exec("CREATE USER a@localhost"); err != nil {
		t.Fatalf("executing DML on master to fail replication: %v", err)
	}

	goodSlave, cleanup := startSlaveInstance(t, master)
	defer cleanup()

	tests := []struct {
		msg        string
		db         *sql.DB
		online     bool
		configured bool
		runningIO  bool
		runningSQL bool
	}{
		{
			msg:        "offline",
			db:         offline,
			online:     false,
			configured: false,
			runningIO:  false,
			runningSQL: false,
		},
		{
			msg:        "master",
			db:         master,
			online:     true,
			configured: false,
			runningIO:  false,
			runningSQL: false,
		},
		{
			msg:        "stopped slave",
			db:         stoppedSlave,
			online:     true,
			configured: true,
			runningIO:  false,
			runningSQL: false,
		},
		{
			msg:        "failed slave",
			db:         failedSlave,
			online:     true,
			configured: true,
			runningIO:  true,
			runningSQL: false,
		},
		{
			msg:        "running slave",
			db:         goodSlave,
			online:     true,
			configured: true,
			runningIO:  true,
			runningSQL: true,
		},
	}

	for _, test := range tests {
		t.Run(test.msg, func(t *testing.T) {
			status := checkSlaveStatus(test.db, defaultCheckTimeout)
			if status.online != test.online {
				t.Errorf("online, expected %v, got %v", test.online, status.online)
			}
			if status.configured != test.configured {
				t.Errorf("configured, expected %v, got %v", test.configured, status.configured)
			}
			if status.runningIO != test.runningIO {
				t.Errorf("runningIO, expected %v, got %v", test.runningIO, status.runningIO)
			}
			if status.runningSQL != test.runningSQL {
				t.Errorf("runningSQL, expected %v, got %v", test.runningSQL, status.runningSQL)
			}
			if status.latency <= 0 {
				t.Errorf("latency, expected > 0, got %v", status.latency)
			}
		})
	}
}

func TestCheckReadOnlyStatus(t *testing.T) {
	master, cleanup := startMasterInstance(t)
	defer cleanup()
	slave, cleanup := startSlaveInstance(t, nil)
	defer cleanup()
	offline := startOfflineInstance(t)

	tests := []struct {
		msg      string
		db       *sql.DB
		online   bool
		readOnly bool
	}{
		{
			msg:      "master",
			db:       master,
			online:   true,
			readOnly: false,
		},
		{
			msg:      "slave",
			db:       slave,
			online:   true,
			readOnly: true,
		},
		{
			msg:    "offline",
			db:     offline,
			online: false,
		},
	}

	for _, test := range tests {
		t.Run(test.msg, func(t *testing.T) {
			status := checkReadOnlyStatus(test.db, defaultCheckTimeout)

			if status.online != test.online {
				t.Errorf("online, expected %v, got %v", test.online, status.online)
			}
			if status.readOnly != test.readOnly {
				t.Errorf("readOnly, expected %v, got %v", test.readOnly, status.readOnly)
			}
			if status.latency <= 0 {
				t.Errorf("latency, expected > 0, got %v", status.latency)
			}
		})
	}
}

func TestCheckWsrepStatus(t *testing.T) {
	dp := dockerPool(t)
	net, err := dp.CreateNetwork("wsrep")
	if err != nil {
		t.Fatalf("creating docker network for galera: %v", err)
	}
	defer net.Close()

	master, cleanup := startMasterInstance(t)
	defer cleanup()
	node1, cleanup := startGaleraInstance(t)
	defer cleanup()
	node2, cleanup := startGaleraInstance(t, node1)
	defer cleanup()
	node3, cleanup := startGaleraInstance(t, node1, node2)
	defer cleanup()

	tests := []struct {
		msg    string
		db     *sql.DB
		online bool
		ready  bool
	}{
		{
			msg:    "master",
			db:     master,
			online: false,
			ready:  false,
		},
		{
			msg:    "node1",
			db:     node1,
			online: true,
			ready:  true,
		},
		{
			msg:    "node2",
			db:     node2,
			online: true,
			ready:  true,
		},
		{
			msg:    "node3",
			db:     node3,
			online: true,
			ready:  true,
		},
		// Can not find a way to make one of the nodes in a failed state
		// The only reliable way to do it is to cut the nodes network
		// But this makes the node to look offline for tests as well
	}

	for _, test := range tests {
		t.Run(test.msg, func(t *testing.T) {
			status := checkWsrepStatus(test.db, defaultCheckTimeout)

			if status.online != test.online {
				t.Errorf("online, expected %v, got %v", test.online, status.online)
			}
			if status.ready != test.ready {
				t.Errorf("ready, expected %v, got %v", test.ready, status.ready)
			}
			if status.latency <= 0 {
				t.Errorf("latency, expected > 0, got %v", status.latency)
			}
		})
	}
}
