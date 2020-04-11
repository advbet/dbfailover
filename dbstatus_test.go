package dbfailover

import (
	"database/sql"
	"testing"
)

func TestCheckDBStatus(t *testing.T) {
	master, cleanup := startMasterInstance(t)
	defer cleanup()
	slave, cleanup := startSlaveInstance(t)
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
			status := checkDBStatus(test.db)

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
