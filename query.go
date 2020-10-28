package cworm

import (
	"database/sql"
	"encoding/json"
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
	Joins      []interface{}

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
		return db.ReturnError(errors.New("Not found"))
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

	_, err = stmt.Exec(db.Query.Values...)
	if err != nil {
		return db.Return(nil, err)
	}

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

//BuildJoin ...
func (query *Query) BuildJoin(Model interface{}) error {
	modelStruct := reflect.TypeOf(Model)
	field, _ := reflect.TypeOf(query.Model.Interface()).FieldByName(modelStruct.Name())
	foreignKey := field.Tag.Get("foreign_key")
	jsonKey := field.Tag.Get("json")
	jsonObject := field.Tag.Get("json_object")
	if jsonObject != "" {
		modelStruct := reflect.Indirect(reflect.ValueOf(Model))
		tableName := query.getTable(modelStruct)
		var columns []string
		for i := 0; i < modelStruct.NumField(); i++ {
			switch modelStruct.Field(i).Kind() {
			case
				reflect.Struct,
				reflect.Slice,
				reflect.Ptr:
				continue
			}

			key := snakeCase(modelStruct.Type().Field(i).Name)
			columns = append(columns, "'"+key+"'")
			columns = append(columns, tableName+"."+key)
		}

		query.Columns = append(query.Columns, fmt.Sprintf("CONCAT('[',GROUP_CONCAT(JSON_OBJECT(%s)),']') as %s", strings.Join(columns, ","), jsonKey))
		query.Join += fmt.Sprintf(" LEFT JOIN %s on JSON_CONTAINS(%s.%s, JSON_QUOTE(%s.%s), '$')", jsonKey, query.Table, jsonKey, jsonKey, jsonObject)
		query.GroupBy += fmt.Sprintf(" GROUP BY %s.id", query.Table)

		return nil
	}

	if err := query.mapStruct(Model); err != nil {
		return err
	}

	joinTable, err := getTableName(Model)
	if err != nil {
		return err
	}

	query.Join += fmt.Sprintf(" LEFT JOIN %s ON %s.id=%s.%s", joinTable, joinTable, query.Table, foreignKey)

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
func (query *Query) fillModel(Model reflect.Value, values []sql.RawBytes, index int) (err error) {
	defer func() {
		if err2 := recover(); err2 != nil {
			err = errors.New(fmt.Sprintf("Exception: %v", err2))
		}
	}()

	for i := 0; i < Model.NumField(); i++ {
		field := Model.Type().Field(i)
		fieldName := field.Name
		structField := Model.FieldByName(fieldName)

		if !structField.CanSet() {
			continue
		}

		structKind := structField.Type().Kind()
		if structKind == reflect.Slice || structKind == reflect.Struct || structKind == reflect.Ptr {
			structType := reflect.TypeOf(structField.Interface())
			if structKind == reflect.Slice {
				structType = structType.Elem()
			}

			var modelStruct reflect.Value

			if structKind == reflect.Ptr {
				test := reflect.New(structType.Elem()).Interface()
				test2 := reflect.ValueOf(test).Elem()
				if test2.Type().Kind() == reflect.Slice {
					modelStruct = reflect.New(test2.Type().Elem()).Elem()
				} else {
					modelStruct = reflect.New(test2.Type()).Elem()
				}
			} else {
				modelStruct = reflect.New(structType).Elem()
			}

			hasJoin := false
			for _, joinModel := range query.Joins {
				if joinModel == modelStruct.Interface() {
					hasJoin = true

					jsonObject := field.Tag.Get("json_object")
					if jsonObject != "" {
						data := reflect.New(structType.Elem()).Interface()
						json.Unmarshal(values[index+i], &data)
						structField.Set(reflect.ValueOf(data))
					} else {
						if err := query.fillModel(modelStruct, values, index+i); err != nil {
							return err
						}

						field := reflect.New(reflect.TypeOf(joinModel))
						field.Elem().Set(reflect.ValueOf(modelStruct.Interface()))
						structField.Set(field)
					}

					break
				}
			}

			if !hasJoin {
				//fmt.Printf("%T does not have join\n", modelStruct.Interface())
			}

			continue

		}

		err := query.fillField(index+i, structField, fieldName, values)
		if err != nil {
			return errors.New(err.Error())
		}
	}

	return nil
}

//fillField ...9
func (query *Query) fillField(index int, structField reflect.Value, fieldName string, values []sql.RawBytes) (err error) {
	defer func() {
		if err2 := recover(); err2 != nil {
			err = errors.New(fmt.Sprintf("Exception: %v\n", err2))
		}
	}()

	var v interface{}

	//fmt.Printf("%v: %#v – %T\n", index, index, index)

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

	// fmt.Printf("%v: %#v – %T\n", fieldName, v, v)
	// fmt.Println("--------------")

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
		field := modelStruct.Type().Field(i)
		fieldName := field.Name
		structField := modelStruct.FieldByName(fieldName)
		structFieldKind := modelStruct.Field(i).Kind()

		if structFieldKind == reflect.Slice || structFieldKind == reflect.Struct || structFieldKind == reflect.Ptr {
			structType := reflect.TypeOf(structField.Interface())
			if structFieldKind == reflect.Slice {
				structType = structType.Elem()
			}

			var modelStruct reflect.Value

			if structFieldKind == reflect.Ptr {
				test := reflect.New(structType.Elem()).Interface()
				test2 := reflect.ValueOf(test).Elem()
				if test2.Type().Kind() == reflect.Slice {
					modelStruct = reflect.New(test2.Type().Elem()).Elem()
				} else {
					modelStruct = reflect.New(test2.Type()).Elem()
				}
			} else {
				modelStruct = reflect.New(structType).Elem()
			}

			hasJoin := false
			for _, joinModel := range query.Joins {
				if joinModel == modelStruct.Interface() {
					hasJoin = true
					query.BuildJoin(joinModel)
					break
				}
			}

			if !hasJoin {
				jsonKey := field.Tag.Get("json")
				query.Columns = append(query.Columns, "\"\" as "+snakeCase(jsonKey))
				query.Values = append(query.Values, nil)
			}

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
