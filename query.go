package worm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type Query struct {
	Columns []string
	Params  []string
	Values  []interface{}
	Args    []interface{}

	Select  string
	Where   string
	Join    string
	GroupBy string
	Having  string
	OrderBy string
	Limit   string
	Offset  string

	Table      string
	Conditions []interface{}
	Joins      map[string]interface{}

	Model reflect.Value
}

type Where struct {
	Column   string
	Operator string
	Value    interface{}
}

//TODO:
// Finished getting mapper working, need to fill struct now w/ query
// [ ] Add/fix JOIN order, currently alphabetical and not specified order.
// [ ] Figure out how to not require a Ptr value on .First()/.Get()/.Insert()

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

func (db *DB) Join(Model interface{}, foreignKey string) *DB {
	if db.Query.Joins == nil {
		db.Query.Joins = make(map[string]interface{})
	}

	db.Query.Joins[foreignKey] = Model

	return db
}

func (db *DB) Where(column string, operator string, value interface{}) *DB {
	db.Query.Conditions = append(db.Query.Conditions, Where{Column: column, Operator: operator, Value: value})

	return db
}

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

func (db *DB) OrderBy(column string, order string) *DB {
	if db.Query.OrderBy == "" {
		db.Query.OrderBy = fmt.Sprintf(" ORDER BY %s %s", column, order)
	} else {
		db.Query.OrderBy += fmt.Sprintf(",%s %s", column, order)
	}

	return db
}

func (db *DB) Limit(limit int) *DB {
	db.Query.Limit = fmt.Sprintf(" LIMIT %d", limit)

	return db
}

func (db *DB) Offset(offset int) *DB {
	db.Query.Offset = fmt.Sprintf(" OFFSET %d", offset)

	return db
}

func (db *DB) Exists(Model interface{}) (exists bool, err error) {
	db.Query.Table, err = getTableName(Model)
	if err != nil {
		return false, err
	}

	sql, err := db.Select("1").Query.BuildSelect()
	if err != nil {
		return false, err
	}

	sql = fmt.Sprintf("SELECT EXISTS(%s LIMIT 1)", sql)

	err = db.QueryRow(sql, db.Query.Args...).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("Error checking if row exists %v", err)
	}

	return exists, nil
}

func (db *DB) First(Model interface{}) (interface{}, error) {
	rows, err := db.Limit(1).Get(Model)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *DB) Get(Model interface{}) ([]interface{}, error) {
	if db.HasErrors() {
		return nil, db.ErrorMessages()
	}

	if err := db.Query.mapStruct(Model); err != nil {
		return nil, err
	}

	sql, err := db.Query.BuildSelect()
	if err != nil {
		return nil, err
	}

	// fmt.Println(sql)

	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(db.Query.Args...)
	if err != nil {
		return nil, err
	}

	results, err := db.Query.fillRows(rows)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (db *DB) New(Model interface{}) (interface{}, error) {
	if db.HasErrors() {
		return nil, db.ErrorMessages()
	}

	if err := db.Query.mapStruct(Model); err != nil {
		return nil, err
	}

	sql, err := db.Query.BuildInsert()
	if err != nil {
		return nil, err
	}

	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}

	res, err := stmt.Exec(db.Query.Values...)
	if err != nil {
		return nil, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	if db.Query.Model.FieldByName("Id").CanSet() {
		db.Query.Model.FieldByName("Id").Set(reflect.ValueOf(int(id)))
	}
	//Query to get CreatedAt/UpdatedAt?

	return db.Query.Model, nil
}

func getTableName(Model interface{}) (string, error) {
	modelStruct := reflect.TypeOf(Model)
	if modelStruct.Kind() != reflect.Struct {
		return "", errors.New("Model given is not a struct")
	}

	return pluralizeString(snakeCase(modelStruct.Name())), nil
}

func (query *Query) getTable(Model reflect.Value) string {
	return pluralizeString(snakeCase(Model.Type().Name()))
}

func (query *Query) getParams() string {
	return strings.Join(query.Params, ",")
}

func (query *Query) getColumns() string {
	return strings.Join(query.Columns, ",")
}

func (query *Query) mapStruct(Model interface{}) error {
	modelStruct := reflect.Indirect(reflect.ValueOf(Model))
	if modelStruct.Kind() != reflect.Struct {
		return errors.New("Model given is not a struct")
	}

	tableName := query.getTable(modelStruct)

	if !query.Model.IsValid() {
		query.Model = modelStruct
		query.Table = tableName
	}

	var v interface{}

	for i := 0; i < modelStruct.NumField(); i++ {
		if modelStruct.Field(i).Kind() == reflect.Struct {
			continue
		}

		key := modelStruct.Type().Field(i).Name
		val := modelStruct.Field(i).Interface()

		if val == "" {
			v = nil
		} else {
			v = val
		}

		query.Columns = append(query.Columns, tableName+"."+snakeCase(key))
		query.Params = append(query.Params, "?")
		query.Values = append(query.Values, v)
	}

	return nil
}
