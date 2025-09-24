package dbdt

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var activeDB = "./data.db"

func SetActiveDB(dbName string) {
	if activeFolder == "" {
		panic("activeFolder not set")
	}

	activeDB = filepath.Join(activeFolder, dbName)
}

func SetActiveDBPath(dbPath string) {
	activeDB = dbPath
}

func ActiveDBPath() string {
	return activeDB
}

func OpenActiveDB() (*sql.DB, error) {
	if activeFolder == "" {
		return nil, errors.New("folder not initialised")
	}

	if activeDB == "" {
		return nil, errors.New("active DB not set")
	}

	return OpenDB(activeDB)
}

func OpenDB(dbPath string) (*sql.DB, error) {
	return sql.Open("sqlite3", dbPath)
}

func getDBAffinity(field reflect.StructField) string {
	kind := field.Type.Kind().String()

	if kind == "string" {
		return "TEXT"
	}

	if kind == "bool" {
		return "BOOL"
	}

	if strings.HasPrefix(kind, "int") {
		return "INTEGER"
	}

	if strings.HasPrefix(kind, "float") {
		return "REAL"
	}

	if kind == "Array" {
		elemKind := field.Type.Elem().Kind()

		if elemKind == reflect.Uint8 {
			return "BLOB"
		}
	}

	return "ANY"
}

func GetTableName(entityType reflect.Type) string {
	name := entityType.Name()

	if strings.HasSuffix(name, "s") {
		return name
	}

	if strings.HasSuffix(name, "y") {
		return name[:len(name)-1] + "ies"
	}

	return name + "s"
}

func CreateTable[T any]() error {
	db, err := OpenActiveDB()

	if err != nil {
		return err
	}

	defer db.Close()

	return CreateTableDB[T](db)
}

func getExportedFields(targetType reflect.Type) []reflect.StructField {
	fields := reflect.VisibleFields(targetType)

	permittedKinds := []reflect.Kind{
		reflect.Bool,
		reflect.Float64,
		reflect.Int,
		reflect.Int64,
		reflect.String,
	}

	exportedFields := []reflect.StructField{}

	for _, field := range fields {
		if !field.IsExported() {
			continue
		}

		kind := field.Type.Kind()

		if kind == reflect.Array || kind == reflect.Slice {
			elementKind := field.Type.Elem().Kind()

			if elementKind == reflect.Uint8 {
				exportedFields = append(exportedFields, field)
			}

			continue
		}

		if slices.Contains(permittedKinds, kind) {
			exportedFields = append(exportedFields, field)
		}
	}

	return exportedFields
}

func CreateTableDB[T any](db *sql.DB) error {
	targetType := reflect.TypeFor[T]()
	tableName := GetTableName(targetType)
	fields := getExportedFields(targetType)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\n", tableName)

	primaryKeyAdded := false

	for i, field := range fields {
		affinity := getDBAffinity(field)

		query += field.Name + " " + affinity

		if !primaryKeyAdded && field.Name == "ID" {
			query += " PRIMARY KEY"
			primaryKeyAdded = true
		}

		if i != len(fields)-1 {
			query += ","
		}

		query += "\n"
	}

	query += ");"

	return ExecDB(db, query)
}

func Insert[T any](entity *T) error {
	db, err := OpenActiveDB()

	if err != nil {
		return err
	}

	defer db.Close()

	return InsertDB(db, entity)
}

func Add[T any](entity T) error {
	ptr := &entity
	return Insert(ptr)
}

func AddDB[T any](db *sql.DB, entity T) error {
	ptr := &entity
	return InsertDB(db, ptr)
}

func InsertDB[T any](db *sql.DB, entity *T) error {
	targetType := reflect.TypeOf(*entity)
	tableName := GetTableName(targetType)
	fields := getExportedFields(targetType)
	reflectValue := reflect.ValueOf(*entity)
	entityValues := make([]any, len(fields))

	awaitingRowID := false

	query := "INSERT INTO \"" + tableName + "\" VALUES ("

	for i, field := range fields {
		query += "?"

		if i != len(fields)-1 {
			query += ", "
		}

		if field.Name != "ID" {
			entityValues[i] = reflectValue.FieldByIndex(field.Index).Interface()
		} else {
			idValue := reflectValue.FieldByIndex(field.Index).Interface()

			idString := fmt.Sprint(idValue)

			if idString == "0" {
				entityValues[i] = nil
				awaitingRowID = true
			} else {
				entityValues[i] = idValue
			}
		}
	}

	query += ");"

	res, err := db.Exec(query, entityValues...)

	if err != nil {
		return err
	}

	if awaitingRowID {
		entityID, err := res.LastInsertId()

		if err != nil {
			return err
		}

		reflectValue := reflect.ValueOf(entity).Elem()
		idField := reflectValue.FieldByName("ID")
		idField.SetInt(entityID)
	}

	return nil
}

func AddAll[T any](entities []T) error {
	entityPtrs := make([]*T, len(entities))

	for i := 0; i < len(entities); i++ {
		entityPtrs[i] = &entities[i]
	}

	return InsertAll(entityPtrs)
}

func InsertAll[T any](entities []*T) error {
	db, err := OpenActiveDB()

	if err != nil {
		return err
	}

	defer db.Close()

	return InsertAllDB(db, entities)
}

func InsertAllDB[T any](db *sql.DB, entities []*T) error {
	targetType := reflect.TypeFor[T]()
	tableName := GetTableName(targetType)
	fields := getExportedFields(targetType)

	useRowID := false
	idFieldIndex := []int{}

	query := "INSERT INTO \"" + tableName + "\" VALUES ("

	for i, field := range fields {
		query += "?"

		if i != len(fields)-1 {
			query += ", "
		}

		if field.Name == "ID" {
			if field.Type.Kind() == reflect.Int {
				useRowID = true
				idFieldIndex = field.Index
			}
		}
	}

	query += ");"

	transaction, err := db.Begin()

	if err != nil {
		return err
	}

	stmt, err := db.Prepare(query)

	if err != nil {
		return err
	}

	defer stmt.Close()

	for _, entityPtr := range entities {
		entityValues := reflect.ValueOf(entityPtr).Elem() // Since ValueOf is targeting a pointer, use Elem to get/set underlying struct
		parameters := make([]any, len(fields))
		awaitingRowID := useRowID

		if useRowID {
			// Check if ID is currently 0, if so, send nil
			idValue := entityValues.FieldByIndex(idFieldIndex).Interface()
			idString := fmt.Sprint(idValue)

			if idString != "0" {
				awaitingRowID = false
			}
		}

		for i, field := range fields {
			// If expecting rowID to be set by database, send nil
			if awaitingRowID && field.Index[0] == idFieldIndex[0] {
				parameters[i] = nil
				continue
			}

			parameters[i] = entityValues.FieldByIndex(field.Index).Interface()
		}

		res, err := stmt.Exec(parameters...)

		if err != nil {
			return err
		}

		if awaitingRowID {
			entityID, err := res.LastInsertId()

			if err != nil {
				return err
			}

			idField := entityValues.FieldByIndex(idFieldIndex)
			idField.SetInt(entityID)
		}
	}

	return transaction.Commit()
}

func Update[T any](entity T) error {
	db, err := OpenActiveDB()

	if err != nil {
		return err
	}

	defer db.Close()

	return UpdateDB(db, entity)
}

func UpdateDB[T any](db *sql.DB, entity T) error {
	targetType := reflect.TypeFor[T]()
	tableName := GetTableName(targetType)
	fields := getExportedFields(targetType)
	entityValues := reflect.ValueOf(entity)
	parameters := []any{}

	var entityID any

	query := "UPDATE " + tableName + " SET "

	for i, field := range fields {
		if field.Name != "ID" {
			query += field.Name + " = ?"

			if i != len(fields)-1 {
				query += ", "
			}

			value := entityValues.FieldByIndex(field.Index).Interface()
			parameters = append(parameters, value)
		} else {
			entityID = entityValues.FieldByIndex(field.Index).Interface()
		}
	}

	if entityID == nil {
		return errors.New("cannot update entity, ID not set")
	}

	parameters = append(parameters, entityID)

	query += " WHERE ID = ?;"

	return ExecDB(db, query, parameters...)
}

func UpdateAll[T any](db *sql.DB, entities []T) error {
	targetType := reflect.TypeFor[T]()
	tableName := GetTableName(targetType)
	fields := getExportedFields(targetType)

	query := "UPDATE " + tableName + " SET "

	for i, field := range fields {
		if field.Name != "ID" {
			query += field.Name + " = ?"

			if i != len(fields)-1 {
				query += ", "
			}
		}
	}

	query += " WHERE ID = ?;"

	stmt, err := db.Prepare(query)

	if err != nil {
		return err
	}

	defer stmt.Close()

	transaction, err := db.Begin()

	if err != nil {
		return err
	}

	for _, entity := range entities {
		entityValues := reflect.ValueOf(entity)
		var entityID any
		parameters := make([]any, len(fields))
		paramIndex := 0 // Differs from i, since the ID value goes last

		for i, field := range fields {
			if field.Name != "ID" {
				query += field.Name + " = ?"

				if i != len(fields)-1 {
					query += ", "
				}

				value := entityValues.FieldByIndex(field.Index).Interface()
				parameters[paramIndex] = value
				paramIndex++
			} else {
				entityID = entityValues.FieldByIndex(field.Index).Interface()
			}
		}

		if entityID == nil {
			transaction.Rollback()
			return errors.New("cannot update entity, ID not set")
		}

		parameters[paramIndex] = entityID

		_, err := stmt.Exec(parameters)

		if err != nil {
			transaction.Rollback()
			return err
		}
	}

	return transaction.Commit()
}

func Get[T any](id any) (T, error) {
	db, err := OpenActiveDB()

	if err != nil {
		return *new(T), err
	}

	defer db.Close()

	return GetDB[T](db, id)
}

func GetDB[T any](db *sql.DB, id any) (T, error) {
	targetType := reflect.TypeFor[T]()
	tableName := GetTableName(targetType)

	query := "SELECT * FROM " + tableName + " WHERE ID = ? LIMIT 1"

	entities, err := FindAllDB[T](db, query, id)

	if err != nil {
		return *new(T), err
	}

	if len(entities) == 0 {
		return *new(T), fmt.Errorf("no rows found with ID = %v", id)
	}

	return entities[0], err
}

func GetAll[T any]() ([]T, error) {
	db, err := OpenActiveDB()

	if err != nil {
		return nil, err
	}

	defer db.Close()

	return GetAllDB[T](db)
}

func GetAllDB[T any](db *sql.DB) ([]T, error) {
	targetType := reflect.TypeFor[T]()
	tableName := GetTableName(targetType)

	query := "SELECT * FROM " + tableName

	return FindAllDB[T](db, query)
}

func FindAll[T any](query string, args ...any) ([]T, error) {
	db, err := OpenActiveDB()

	if err != nil {
		return nil, err
	}

	defer db.Close()

	return FindAllDB[T](db, query, args...)
}

func FindAllDB[T any](db *sql.DB, query string, args ...any) ([]T, error) {
	grid, err := GetGridDB(db, query, args...)

	if err != nil {
		return nil, err
	}

	output := make([]T, len(grid.Rows))

	targetType := reflect.TypeFor[T]()

	fieldIndexByColumn := map[string][]int{}

	for _, column := range grid.Columns {
		field, ok := targetType.FieldByName(column)

		if !ok {
			continue
		}

		fieldIndexByColumn[column] = field.Index
	}

	for rowIndex, rowValues := range grid.Rows {
		entity := *new(T)

		reflectValue := reflect.ValueOf(&entity).Elem()

		for columnIndex, column := range grid.Columns {
			value := rowValues[columnIndex]
			fieldIndex, ok := fieldIndexByColumn[column]

			if !ok {
				continue
			}

			field := reflectValue.FieldByIndex(fieldIndex)

			// Handle bool special case, stored as int
			if field.Type().Kind() == reflect.Bool {
				intBool := value.(int64)

				if intBool == 1 {
					field.SetBool(true)
				} else {
					field.SetBool(false)
				}

				continue
			}

			switch v := value.(type) {
			case string:
				field.SetString(v)
			case int64:
				field.SetInt(v)
			case float64:
				field.SetFloat(v)
			case []byte:
				field.SetBytes(v)
			}
		}

		output[rowIndex] = entity
	}

	return output, nil
}

func Exec(query string, args ...any) error {
	db, err := OpenActiveDB()

	if err != nil {
		return err
	}

	defer db.Close()

	return ExecDB(db, query, args...)
}

func ExecDB(db *sql.DB, query string, args ...any) error {
	_, err := db.Exec(query, args...)

	return err
}

func GetSingle[T any](query string, args ...any) (T, error) {
	db, err := OpenActiveDB()

	if err != nil {
		return *new(T), err
	}

	defer db.Close()

	return GetSingleDB[T](db, query, args...)
}

func GetSingleDB[T any](db *sql.DB, query string, args ...any) (T, error) {
	row := db.QueryRow(query, args...)

	value := *new(T)

	err := row.Scan(&value)

	if err != nil {
		return *new(T), err
	}

	return value, nil
}

type Grid struct {
	Columns []string
	Rows    [][]any
}

func GetGrid(query string, args ...any) (Grid, error) {
	db, err := OpenActiveDB()

	if err != nil {
		return Grid{}, err
	}

	defer db.Close()

	return GetGridDB(db, query, args...)
}

func GetGridDB(db *sql.DB, query string, args ...any) (Grid, error) {
	rows, err := db.Query(query, args...)

	if err != nil {
		return Grid{}, err
	}

	defer rows.Close()

	columns, err := rows.Columns()

	if err != nil {
		return Grid{}, err
	}

	numColumns := len(columns)

	pointers := make([]any, numColumns)
	rowValues := make([]any, numColumns)

	for i := range pointers {
		pointers[i] = &rowValues[i] // Assign pointers to elements of the container slice
	}

	output := Grid{columns, [][]any{}}

	for rows.Next() {
		err = rows.Scan(pointers...)

		if err != nil {
			return Grid{}, err
		}

		row := make([]any, numColumns)
		copy(row, rowValues)

		output.Rows = append(output.Rows, row)
	}

	return output, nil
}

func GetRows(query string, args ...any) ([]map[string]any, error) {
	db, err := OpenActiveDB()

	if err != nil {
		return nil, err
	}

	defer db.Close()

	return GetRowsDB(db, query, args...)
}

func GetRowsDB(db *sql.DB, query string, args ...any) ([]map[string]any, error) {
	rows, err := db.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	numColumns := len(columns)

	pointers := make([]any, numColumns)
	values := make([]any, numColumns)

	for i := range pointers {
		pointers[i] = &values[i] // Assign pointers to elements of the container slice
	}

	outputRows := []map[string]any{}

	for rows.Next() {
		err = rows.Scan(pointers...)

		if err != nil {
			return nil, err
		}

		outputRow := map[string]any{}

		for i := range numColumns {
			column := columns[i]
			value := values[i]
			outputRow[column] = value
		}

		outputRows = append(outputRows, outputRow)
	}

	return outputRows, nil
}

func GetRow(query string, args ...any) (map[string]any, error) {
	db, err := OpenActiveDB()

	if err != nil {
		return nil, err
	}

	defer db.Close()

	return GetRowDB(db, query, args...)
}

func GetRowDB(db *sql.DB, query string, args ...any) (map[string]any, error) {
	rows, err := db.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	numColumns := len(columns)

	pointers := make([]any, numColumns)
	values := make([]any, numColumns)

	for i := range pointers {
		pointers[i] = &values[i] // Assign pointers to elements of the container slice
	}

	rows.Next()
	err = rows.Scan(pointers...)

	if err != nil {
		return nil, err
	}

	outputRow := map[string]any{}

	for i := range numColumns {
		column := columns[i]
		value := values[i]
		outputRow[column] = value
	}

	return outputRow, nil
}

func GetColumn[T any](query string, args ...any) ([]T, error) {
	db, err := OpenActiveDB()

	if err != nil {
		return nil, err
	}

	defer db.Close()

	return GetColumnDB[T](db, query, args...)
}

func GetColumnDB[T any](db *sql.DB, query string, args ...any) ([]T, error) {
	rows, err := db.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	value := *new(T)
	values := []T{}

	for rows.Next() {
		err := rows.Scan(&value)

		if err != nil {
			return nil, err
		}

		values = append(values, value)
	}

	return values, nil
}
