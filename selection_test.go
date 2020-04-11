package dbfailover

import (
	"database/sql"
	"testing"
	"time"
)

func TestMakeSelection(t *testing.T) {
	db1 := &sql.DB{}
	db2 := &sql.DB{}
	db3 := &sql.DB{}

	tests := []struct {
		msg        string
		states     map[*sql.DB]dbStatus
		lastMaster *sql.DB
		want       selection
	}{
		{
			msg: "nil",
		},
		{
			msg: "single master",
			states: map[*sql.DB]dbStatus{
				db1: {online: true, readOnly: false},
			},
			want: selection{
				master:     db1,
				slave:      db1,
				lastMaster: db1,
			},
		},
		{
			msg:        "keep lastMaster",
			states:     map[*sql.DB]dbStatus{},
			lastMaster: db1,
			want: selection{
				master:     nil,
				slave:      nil,
				lastMaster: db1,
			},
		},
		{
			msg: "one_master_one_slave",
			states: map[*sql.DB]dbStatus{
				db1: {online: true, readOnly: false},
				db2: {online: true, readOnly: true},
			},
			want: selection{
				master:     db1,
				slave:      db2,
				lastMaster: db1,
			},
		},
		{
			msg: "one master two slaves pick lowest latency",
			states: map[*sql.DB]dbStatus{
				db1: {online: true, readOnly: false, latency: 1 * time.Second},
				db2: {online: true, readOnly: true, latency: 5 * time.Second},
				db3: {online: true, readOnly: true, latency: 2 * time.Second},
			},
			want: selection{
				master:     db1,
				slave:      db3,
				lastMaster: db1,
			},
		},
		{
			msg: "two masters one slave pick lowest latency",
			states: map[*sql.DB]dbStatus{
				db1: {online: true, readOnly: false, latency: 5 * time.Second},
				db2: {online: true, readOnly: false, latency: 2 * time.Second},
				db3: {online: true, readOnly: true, latency: 1 * time.Second},
			},
			want: selection{
				master:     db2,
				slave:      db3,
				lastMaster: db2,
			},
		},
		{
			msg: "slave only",
			states: map[*sql.DB]dbStatus{
				db1: {online: true, readOnly: true},
			},
			lastMaster: db2,
			want: selection{
				master:     nil,
				slave:      db1,
				lastMaster: db2,
			},
		},
		{
			msg: "offline only",
			states: map[*sql.DB]dbStatus{
				db1: {online: false},
				db2: {online: false},
				db3: {online: false},
			},
			want: selection{
				master:     nil,
				slave:      nil,
				lastMaster: nil,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.msg, func(t *testing.T) {
			actual := makeSelection(test.states, test.lastMaster)
			if actual != test.want {
				t.Errorf("expected %v, got %v", test.want, actual)
			}
		})
	}
}
