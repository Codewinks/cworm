package cworm

import (
	"database/sql"
	"errors"
	"fmt"
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
func Connect(dialect string, username string, password string, host string, port string, database string) (worm *DB, err error) {
	// fmt.Printf("%s:%s@tcp(%s:%s)/%s\n", username, password, host, port, database)
	if dialect == "" || username == "" || host == "" || port == "" || database == "" {
		return nil, errors.New("Missing database credentials")
	}
	worm = &DB{}
	worm.DB, err = sql.Open(dialect, fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))

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
