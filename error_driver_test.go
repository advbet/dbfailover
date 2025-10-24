package dbfailover

import (
	"errors"
	"testing"
)

func TestNewFaultyTopologyErrDriver(t *testing.T) {
	db := newMultipleMasterErrConn()
	if db == nil {
		t.Fatal("connection is nil")
	}

	_, err := db.Exec("SELECT 1")
	if !errors.Is(err, ErrMultipleMasters) {
		t.Errorf("expected %v, got %v", ErrMultipleMasters, err)
	}
}
