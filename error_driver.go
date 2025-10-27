package dbfailover

import (
	"database/sql"
	"database/sql/driver"
	"errors"
)

func init() {
	sql.Register("dbfailover_err_driver", errorDriver{})
}

// newMultipleMasterErrConn returns a *sql.DB connection which upon usage will
// always return a ErrMultipleMasters error.
func newMultipleMasterErrConn() *sql.DB {
	db, _ := sql.Open("dbfailover_err_driver", ErrMultipleMasters.Error())
	return db
}

type errorDriver struct{}

func (errorDriver) Open(err string) (driver.Conn, error) {
	if err == ErrMultipleMasters.Error() {
		return nil, ErrMultipleMasters
	}

	return nil, errors.New(err)
}
