package cworm

import (
	"errors"
	"fmt"
	"reflect"
)

//Where ...
type Where struct {
	Column   string
	Operator string
	Value    interface{}
}

//TODO:
// Finished getting mapper working, need to fill struct now w/ query
// [ ] Add/fix JOIN order, currently alphabetical and not specified order.
// [ ] Figure out how to not require a Ptr value on .First()/.Get()/.Insert()

//ResetQuery ...
func (db *DB) ResetQuery() {
	db.Query = Query{}
}

//Select ...
func (db *DB) Select(columns ...string) *DB {
	for _, column := range columns {
		if db.Query.Select == "" {
			db.Query.Select = fmt.Sprintf("SELECT %s", column)
		} else {
			db.Query.Select += fmt.Sprintf(",%s", column)
		}
	}

	return db
}

//Join ...
func (db *DB) Join(Model interface{}, foreignKey string) *DB {
	if db.Query.Joins == nil {
		db.Query.Joins = make(map[string]interface{})
	}

	db.Query.Joins[foreignKey] = Model

	return db
}

//Where ...
func (db *DB) Where(column string, operator string, value interface{}) *DB {
	db.Query.Conditions = append(db.Query.Conditions, Where{Column: column, Operator: operator, Value: value})

	return db
}

//GroupBy ...
func (db *DB) GroupBy(columns ...string) *DB {
	for _, column := range columns {
		if db.Query.GroupBy == "" {
			db.Query.GroupBy = fmt.Sprintf(" GROUP BY %s", column)
		} else {
			db.Query.GroupBy += fmt.Sprintf(",%s", column)
		}
	}

	return db
}

//OrderBy ...
func (db *DB) OrderBy(column string, order string) *DB {
	if db.Query.OrderBy == "" {
		db.Query.OrderBy = fmt.Sprintf(" ORDER BY %s %s", column, order)
	} else {
		db.Query.OrderBy += fmt.Sprintf(",%s %s", column, order)
	}

	return db
}

//Limit ...
func (db *DB) Limit(limit int) *DB {
	db.Query.Limit = fmt.Sprintf(" LIMIT %d", limit)

	return db
}

//Offset ...
func (db *DB) Offset(offset int) *DB {
	db.Query.Offset = fmt.Sprintf(" OFFSET %d", offset)

	return db
}

//getTableName ...
func getTableName(Model interface{}) (string, error) {
	modelStruct := reflect.TypeOf(Model)

	if modelStruct.Kind() == reflect.Ptr {
		modelStruct = modelStruct.Elem()
	}

	if modelStruct.Kind() != reflect.Struct {
		return "", errors.New("Model given is not a struct")
	}

	return pluralizeString(snakeCase(modelStruct.Name())), nil
}
