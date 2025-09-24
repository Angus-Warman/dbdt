package dbdt

import (
	"database/sql"
	"errors"
	"log"
	"path/filepath"
)

const kvDB = "__kv.db"

var kvDBPath = filepath.Join(".", kvDB)

func logError(err error) {
	log.Printf("Error with %v, %v", kvDBPath, err)
}

func initKV() error {
	if activeFolder == "" {
		return errors.New("cannot use KV functions, activeFolder not set")
	}

	if kvDBPath == "" {
		return errors.New("KV db path not set")
	}

	kvDBPath = filepath.Join(activeFolder, kvDB)

	query := "CREATE TABLE IF NOT EXISTS key_values (row_key TEXT PRIMARY KEY, row_value ANY)"

	db, err := OpenDB(kvDBPath)

	if err != nil {
		return err
	}

	err = ExecDB(db, query)

	if err != nil {
		return err
	}

	return nil
}

var initialised = false

func checkInit() {
	if initialised {
		return
	}

	err := initKV()

	if err != nil {
		logError(err)
		return
	}

	initialised = true
}

func SetValue(key string, value string) {
	checkInit()

	query := "INSERT OR REPLACE INTO key_values (row_key, row_value) VALUES (?, ?)"

	db, err := OpenDB(kvDBPath)

	if err != nil {
		logError(err)
		return
	}

	err = ExecDB(db, query, key, value)

	if err != nil {
		logError(err)
		return
	}
}

func GetValue(key string) string {
	checkInit()

	if key == "" {
		return ""
	}

	db, err := OpenDB(kvDBPath)

	if err != nil {
		logError(err)
		return ""
	}

	query := "SELECT row_value FROM key_values WHERE row_key = ? LIMIT 1"

	value, err := GetSingleDB[string](db, query, key)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Do nothing
		} else {
			logError(err)
		}

		return ""
	}

	return value
}
