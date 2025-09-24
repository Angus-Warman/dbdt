# dbdt

Sometimes you don't need object relational management.

Sometimes you just want to store _data_ in a _base_.

This package is probably not suitable for production.

This is _database duct tape_.

---

dbdt is just a couple of thin wrappers around [database/sql](https://pkg.go.dev/database/sql) and a [SQLite driver](https://github.com/mattn/go-sqlite3).

---

## Really Quick Start

```
package main

import (
	"fmt"

	"github.com/Angus-Warman/dbdt"
)

func main() {
	key := "key"
	value := "data to store"

	dbdt.SetValue(key, value)

	// some time later...

	retValue := dbdt.GetValue(key)

	fmt.Println(retValue)
}
```

---

## Quick Start

```
package main

import (
	"fmt"

	"github.com/Angus-Warman/dbdt"
)

type Customer struct {
	ID    int
	Name  string
	Email string
}

func main() {
	dbdt.CreateTable[Customer]()

	alice := Customer{0, "Alice", "alice@email.com"}

	dbdt.Add(alice)
	// dbdt.Insert(&alice) // If you need to use the automatic ROWID, use DBInsert

	newCustomers := []Customer{
		{0, "Bob", ""},
		{106, "Carol", "carol@email.com"}, // specific ID number
	}

	dbdt.AddAll(newCustomers)

	customers, _ := dbdt.GetAll[Customer]()

	fmt.Println(customers)

	validCustomers, _ := dbdt.FindAll[Customer]("SELECT * FROM Customers WHERE Email IS NOT NULL AND Email != ''")

	fmt.Println(validCustomers)
}
```

---

## Start

```
package main

import (
	"fmt"

	"github.com/Angus-Warman/dbdt"
)

func main() {
	dbdt.SetActiveFolder("/path/to/your/data") // Otherwise, current directory

	dbdt.SetActiveDB("name_of_database.db") // Otherwise, "data.db"
}
```