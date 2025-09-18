# dbdt

Sometimes you don't need object relational management.

Sometimes you just want to store _data_ in a _base_.

This package is probably not suitable for production.

This is _database duct tape_.

---

dbdt is just a couple of thin wrappers around [database/sql](https://pkg.go.dev/database/sql) and a [SQLite driver](https://github.com/mattn/go-sqlite3).

To point dbdt at a specific folder or database path, use "SetActiveFolder", "SetActiveDB" and/or "SetActiveDBPath". Defaults are the current directory, and "data.db".

If you just want a key-value store, use the kv functions "SetValue" and "GetValue".
