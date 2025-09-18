package dbdt

import (
	"context"
	"database/sql"
	"time"
)

type DBWatcher struct {
	conn        *sql.Conn
	dataVersion int

	Callbacks []func()
}

func CreateDBWatcher(dbPath string) (*DBWatcher, error) {
	db, err := OpenDB(dbPath)

	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)

	if err != nil {
		return nil, err
	}

	watcher := DBWatcher{conn, 0, []func(){}}

	watcher.checkDataVersion()

	watcher.Start()

	return &watcher, nil
}

func (watcher *DBWatcher) AddCallback(callback func()) {
	watcher.Callbacks = append(watcher.Callbacks, callback)
}

// True if the database has been updated
func (watcher *DBWatcher) checkDataVersion() bool {
	query := "PRAGMA data_version"

	row := watcher.conn.QueryRowContext(context.Background(), query)

	dataVersion := -1

	row.Scan(&dataVersion)

	if dataVersion == -1 {
		return false
	}

	if watcher.dataVersion != dataVersion {
		watcher.dataVersion = dataVersion
		return true
	}

	return false
}

func (watcher *DBWatcher) Start() {
	go func() {
		for {
			time.Sleep(time.Millisecond * 10)

			if watcher.checkDataVersion() {

				// Execute all callbacks
				for _, callback := range watcher.Callbacks {
					callback()
				}

				// Check the data version again, callbacks may have changed DB
				watcher.checkDataVersion()
			}
		}
	}()
}
