package worm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func (query *Query) BuildJoins() error {
	for foreignKey, Model := range query.Joins {
		if err := query.mapStruct(Model); err != nil {
			return err
		}

		joinTable, err := getTableName(Model)
		if err != nil {
			return err
		}

		query.Join += fmt.Sprintf(" INNER JOIN %s ON %s.id=%s.%s", joinTable, joinTable, query.Table, foreignKey)
	}

	return nil
}

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

	return
}

func (query *Query) BuildInsert() (sql string, err error) {
	sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", query.Table, query.getColumns(), query.getParams())
	return
}

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

	if rowCount == 0 {
		return nil, errors.New("Not Found")
	}

	return results, nil
}

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
