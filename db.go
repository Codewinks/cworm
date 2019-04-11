package cworm

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql" //MySQL library package for SQL
)

//TODO:
// [] Check if connection is still alive, if not re-open. (Error checking if row exists '[test-test]' invalid connection)
// [] Optimization - replace sprintf with string concat / buffer / builder

//DB ...
type DB struct {
	*sql.DB
	Query  Query
	Errors []error
}

//Connect establishes a new database connection
func Connect(connection string) (worm *DB, err error) {
	worm = &DB{}
	worm.DB, err = sql.Open("mysql", connection)

	return
}

//HasErrors ...
func (db *DB) HasErrors() bool {
	return len(db.Errors) > 0
}

//ErrorMessages ...
func (db *DB) ErrorMessages() error {
	var msg string
	for _, err := range db.Errors {
		msg += err.Error() + "\n"
	}

	return errors.New(msg)
}

func (db *DB) Error(err error) {
	if err != nil {
		db.Errors = append(db.Errors, err)
	}
}
