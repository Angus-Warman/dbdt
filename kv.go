package dbdt

import (
	"database/sql"
	"errors"
	"path/filepath"
)

const kvDB = "__kv.db"

var kvDBPath = filepath.Join(".", kvDB)

func openKVDB() *sql.DB {
	db, err := OpenDB(kvDBPath)

	if err != nil {
		panic(err)
	}

	return db
}

func initKV() {
	if activeFolder == "" {
		panic("activeFolder not set")
	}

	kvDBPath = filepath.Join(activeFolder, kvDB)

	query := "CREATE TABLE IF NOT EXISTS key_values (row_key TEXT PRIMARY KEY, row_value ANY)"
	db := openKVDB()
	err := Exec(db, query)

	if err != nil {
		panic(err)
	}
}

var initialised = false

func checkInit() {
	if initialised {
		return
	}

	initKV()

	initialised = true
}

func SetValue(key string, value string) {
	checkInit()

	query := "INSERT OR REPLACE INTO key_values (row_key, row_value) VALUES (?, ?)"
	db := openKVDB()
	err := Exec(db, query, key, value)

	if err != nil {
		panic(err)
	}
}

func GetValue(key string) string {
	checkInit()

	query := "SELECT row_value FROM key_values WHERE row_key = ?"
	db := openKVDB()
	value, err := GetSingle[string](db, query, key)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ""
		}

		panic(err)
	}

	return value
}
