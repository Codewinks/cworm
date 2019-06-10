package cworm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

//Query ...
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

//Exists ...
func (db *DB) Exists(Model interface{}) (exists bool, err error) {
	db.Query.Table, err = getTableName(Model)
	if err != nil {
		return db.ReturnBool(false, err)
	}

	sql, err := db.Select("1").Query.BuildSelect()
	if err != nil {
		return db.ReturnBool(false, err)
	}

	sql = fmt.Sprintf("SELECT EXISTS(%s LIMIT 1)", sql)

	fmt.Println(sql)

	err = db.QueryRow(sql, db.Query.Args...).Scan(&exists)
	if err != nil {
		return db.ReturnBool(false, fmt.Errorf("Error checking if row exists %v", err))
	}

	return db.ReturnBool(exists, nil)
}

//First ...
func (db *DB) First(Model interface{}) error {
	rows, err := db.Limit(1).Get(Model)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	return nil
}

//Get ...
func (db *DB) Get(Model interface{}) ([]interface{}, error) {
	if db.HasErrors() {
		return db.ReturnGroup(nil, db.ErrorMessages())
	}

	if err := db.Query.mapStruct(Model); err != nil {
		return db.ReturnGroup(nil, err)
	}

	sql, err := db.Query.BuildSelect()
	if err != nil {
		return db.ReturnGroup(nil, err)
	}

	stmt, err := db.Prepare(sql)
	if err != nil {
		return db.ReturnGroup(nil, err)
	}

	rows, err := stmt.Query(db.Query.Args...)
	if err != nil {
		return db.ReturnGroup(nil, err)
	}

	results, err := db.Query.fillRows(rows)
	if err != nil {
		return db.ReturnGroup(nil, err)
	}

	return db.ReturnGroup(results, nil)
}

//Insert ...
func (db *DB) Insert(Model interface{}) (interface{}, error) {
	if db.HasErrors() {
		return db.Return(nil, db.ErrorMessages())
	}

	if err := db.Query.mapStruct(Model); err != nil {
		return db.Return(nil, err)
	}

	sql, err := db.Query.BuildInsert()
	if err != nil {
		return db.Return(nil, err)
	}

	stmt, err := db.Prepare(sql)
	if err != nil {
		return db.Return(nil, err)
	}

	res, err := stmt.Exec(db.Query.Values...)
	if err != nil {
		return db.Return(nil, err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return db.Return(nil, err)
	}

	if db.Query.Model.FieldByName("Id").CanSet() {
		db.Query.Model.FieldByName("Id").Set(reflect.ValueOf(strconv.FormatInt(id, 10)))
	}
	//Query to get CreatedAt/UpdatedAt?

	return db.Return(db.Query.Model.Interface(), nil)
}

//Delete ...
func (db *DB) Delete(Model interface{}) (int64, error) {
	if db.HasErrors() {
		return db.ReturnInt64(0, db.ErrorMessages())
	}

	if err := db.Query.mapStruct(Model); err != nil {
		return db.ReturnInt64(0, err)
	}

	sql, args, err := db.Query.BuildDelete()
	if err != nil {
		return db.ReturnInt64(0, err)
	}

	stmt, err := db.Prepare(sql)
	if err != nil {
		return db.ReturnInt64(0, err)
	}

	res, err := stmt.Exec(args...)
	if err != nil {
		return db.ReturnInt64(0, err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		return db.ReturnInt64(0, err)
	}

	return db.ReturnInt64(count, nil)
}

//Save ...
func (db *DB) Save(Model interface{}) error {
	if db.HasErrors() {
		return db.ReturnError(db.ErrorMessages())
	}

	if err := db.Query.mapStruct(Model); err != nil {
		return db.ReturnError(err)
	}

	sql, args, err := db.Query.BuildUpdate()
	if err != nil {
		return db.ReturnError(err)
	}
	fmt.Printf("%#v\n", sql)
	fmt.Printf("%#v\n", args)

	stmt, err := db.Prepare(sql)
	if err != nil {
		return db.ReturnError(err)
	}

	res, err := stmt.Exec(args...)
	if err != nil {
		return db.ReturnError(err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		return db.ReturnError(err)
	}

	if count == 0 {
		db.ReturnError(errors.New("No rows updated"))
	}

	return nil
}

//Return – TODO: Refactor to not use return functions to reset query.
func (db *DB) Return(resp interface{}, err error) (interface{}, error) {
	db.ResetQuery()
	return resp, err
}

//ReturnBool – TODO: Refactor to not use return functions to reset query.
func (db *DB) ReturnBool(resp bool, err error) (bool, error) {
	db.ResetQuery()
	return resp, err
}

//ReturnInt64 – TODO: Refactor to not use return functions to reset query.
func (db *DB) ReturnInt64(resp int64, err error) (int64, error) {
	db.ResetQuery()
	return resp, err
}

//ReturnError – TODO: Refactor to not use return functions to reset query.
func (db *DB) ReturnError(err error) error {
	db.ResetQuery()
	return err
}

//ReturnGroup – TODO: Refactor to not use return functions to reset query.
func (db *DB) ReturnGroup(resp []interface{}, err error) ([]interface{}, error) {
	db.ResetQuery()
	return resp, err
}

//BuildJoins ...
func (query *Query) BuildJoins() error {
	for foreignKey, Model := range query.Joins {
		if err := query.mapStruct(Model); err != nil {
			return err
		}

		joinTable, err := getTableName(Model)
		if err != nil {
			return err
		}

		query.Join += fmt.Sprintf(" LEFT JOIN %s ON %s.id=%s.%s", joinTable, joinTable, query.Table, foreignKey)
	}

	return nil
}

//BuildConditions ...
func (query *Query) BuildConditions() error {
	for _, condition := range query.Conditions {
		mapStruct := reflect.TypeOf(condition)
		switch mapStruct.Name() {
		case "Where":
			w := condition.(Where)
			if !strings.Contains(w.Column, ".") && query.Table != "" {
				w.Column = query.Table + "." + w.Column
			}

			if query.Where == "" {
				query.Where = fmt.Sprintf(" WHERE %s %s ?", w.Column, w.Operator)
			} else {
				query.Where += fmt.Sprintf(" AND %s %s ?", w.Column, w.Operator)
			}

			query.Args = append(query.Args, w.Value)
		}
	}

	return nil
}

//BuildSelect ...
func (query *Query) BuildSelect() (sql string, err error) {
	if err = query.BuildConditions(); err != nil {
		return "", err
	}

	if err = query.BuildJoins(); err != nil {
		return "", err
	}

	if query.Select != "" {
		sql = query.Select
	} else {
		sql = "SELECT " + query.getColumns()
	}

	sql += " FROM " + query.Table

	if query.Join != "" {
		sql += query.Join
	}
	if query.Where != "" {
		sql += query.Where
	}
	if query.GroupBy != "" {
		sql += query.GroupBy
	}
	if query.Having != "" {
		sql += query.Having
	}
	if query.OrderBy != "" {
		sql += query.OrderBy
	}
	if query.Offset != "" {
		sql += query.Offset
	}
	if query.Limit != "" {
		sql += query.Limit
	}

	fmt.Println(sql)

	return
}

//BuildInsert ...
func (query *Query) BuildInsert() (sql string, err error) {
	sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", query.Table, query.getColumns(), query.getParams())

	fmt.Println(sql)
	return
}

//BuildUpdate ...
func (query *Query) BuildUpdate() (sql string, args []interface{}, err error) {
	if err = query.BuildConditions(); err != nil {
		return "", nil, err
	}

	sql = fmt.Sprintf("UPDATE %s SET ", query.Table)

	setSQL := []string{}
	for i, col := range query.Columns {
		if col == query.Table+".id" || col == query.Table+".created_at" || col == query.Table+".updated_at" {
			continue
		}
		setSQL = append(setSQL, col+"=?")
		args = append(args, query.Values[i])
	}
	sql += strings.Join(setSQL, ",")

	if query.Where != "" {
		sql += query.Where
	} else {
		sql += " WHERE " + query.Table + ".id=?"
		args = append(args, query.Model.FieldByName("Id").Interface())
	}

	fmt.Println(sql)

	return
}

//BuildDelete ...
func (query *Query) BuildDelete() (sql string, args []interface{}, err error) {
	if err = query.BuildConditions(); err != nil {
		return "", nil, err
	}

	sql = "DELETE FROM " + query.Table

	if query.Where != "" {
		sql += query.Where
	} else {
		sql += " WHERE " + query.Table + ".id=?"
		args = append(args, query.Model.FieldByName("Id").Interface())
	}

	fmt.Println(sql)

	return
}

//fillRows ...
func (query *Query) fillRows(rows *sql.Rows) ([]interface{}, error) {
	values := make([]sql.RawBytes, len(query.Columns))
	scanArgs := make([]interface{}, len(query.Columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	var rowCount int
	var results []interface{}
	var index int

	for rows.Next() {
		rowCount++
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, errors.New(err.Error())
		}

		if err := query.fillModel(query.Model, values, index); err != nil {
			return nil, err
		}

		// fmt.Printf("%#v \n", query.Model.Interface())
		results = append(results, query.Model.Interface())
	}

	if err := rows.Err(); err != nil {
		return nil, errors.New(err.Error())
	}

	// if rowCount == 0 {
	// 	return nil, errors.New("Not Found")
	// }

	return results, nil
}

//fillModel ...
func (query *Query) fillModel(Model reflect.Value, values []sql.RawBytes, index int) error {
	for i := 0; i < Model.NumField(); i++ {
		fieldName := Model.Type().Field(i).Name
		structField := Model.FieldByName(fieldName)

		if !structField.CanSet() {
			continue
		}

		err := query.fillField(index+i, structField, fieldName, values)
		if err != nil {
			return errors.New(err.Error())
		}
	}

	return nil
}

//fillField ...
func (query *Query) fillField(index int, structField reflect.Value, fieldName string, values []sql.RawBytes) error {
	var v interface{}
	var err error

	if structField.Type().Kind() == reflect.Struct {
		for _, Model := range query.Joins {
			if structField.Type().Name() == reflect.TypeOf(Model).Name() {
				if err := query.fillModel(structField, values, index); err != nil {
					return err
				}

				return nil
			}
		}

		return nil
	}

	val := values[index]

	switch structField.Type().Kind() {
	case reflect.Slice:
		v = val
	case reflect.String:
		v = string(val)
	case reflect.Bool:
		v = string(val) == "1"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err = strconv.Atoi(string(val))
		if err != nil {
			return errors.New("Field " + fieldName + " as int: " + err.Error())
		}
	case reflect.Float32, reflect.Float64:
		v, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			return errors.New("Field " + fieldName + " as float64: " + err.Error())
		}
	default:
		return errors.New("Unsupported type in Scan: " + reflect.TypeOf(v).String())
	}

	structField.Set(reflect.ValueOf(v))

	return nil
}

//getTable ...
func (query *Query) getTable(Model reflect.Value) string {
	return pluralizeString(snakeCase(Model.Type().Name()))
}

//getParams ... TODO: possibly remove/refactor - not needed for simple method
func (query *Query) getParams() string {
	return strings.Join(query.Params, ",")
}

//getColumns ... TODO: possibly remove/refactor - not needed for simple method
func (query *Query) getColumns() string {
	return strings.Join(query.Columns, ",")
}

//mapStruct ...
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
