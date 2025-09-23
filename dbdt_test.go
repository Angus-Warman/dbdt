package dbdt

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	runnerDir := os.Getenv("RUNNER_TEMP")

	if runnerDir != "" { // On github actions
		SetActiveFolder(runnerDir)
	} else {
		tempDir := os.TempDir()
		tempFolder := filepath.Join(tempDir, "dbdt_testing")

		err := os.RemoveAll(tempFolder)

		if err != nil {
			panic(err)
		}

		SetActiveFolder(tempFolder)
	}

	SetActiveDB("data.db")

	os.Exit(m.Run())
}

func TestKeyValue(t *testing.T) {

	value := GetValue("test")
	want := ""

	if want != value {
		t.Fatal("GetValue should return empty string if a key doesn't exist")
	}

	SetValue("test", "value")

	value = GetValue("test")
	want = "value"

	if want != value {
		t.Fatal("GetValue should return stored value")
	}
}

type Entity struct {
	ID          int
	Text        string
	Boolean     bool
	Number      int
	OtherNumber float64
	Data        []byte
	hidden      string
	Embedded    Internal
}

type Internal struct {
	InternalText   string
	InternalNumber int
}

func TestCreateTable(t *testing.T) {
	err := DBCreateTable[Entity]()

	if err != nil {
		t.Fatal(err)
	}

	entity := Entity{
		0,
		"hello",
		true,
		1,
		1.5,
		[]byte{1, 2, 3, 4, 5},
		"hidden",
		Internal{"test", 1},
	}

	err = DBInsert(&entity)

	if err != nil {
		t.Fatal(err)
	}

	if entity.ID == 0 {
		t.Fatal("ID not set by insert")
	}

	// Set ID back to 0 and re-use
	entity.ID = 0

	err = DBInsert(&entity)

	if err != nil {
		t.Fatal(err)
	}

	rows, err := DBGetAll[Entity]()

	if err != nil {
		t.Fatal(err)
	}

	for _, entity := range rows {
		id := entity.ID

		if id == 0 {
			t.Fatal("row auto-increment not working")
		}

		wantText := "hello"
		gotText := entity.Text

		if wantText != gotText {
			t.Fatal("text field did not enter correctly")
		}

		if !entity.Boolean {
			t.Fatal("failed to save boolean")
		}

		wantBytes := []byte{1, 2, 3, 4, 5}
		gotBytes := entity.Data

		if !bytes.Equal(wantBytes, gotBytes) {
			t.Fatal("bytes not encoded", wantBytes, gotBytes)
		}
	}
}

func TestUpdate(t *testing.T) {

	err := DBCreateTable[Entity]()

	if err != nil {
		t.Fatal(err)
	}

	entity := Entity{
		0,
		"hello",
		true,
		1,
		1.5,
		[]byte{1, 2, 3, 4, 5},
		"hidden",
		Internal{"test", 1},
	}

	err = DBInsert(&entity)

	if err != nil {
		t.Fatal(err)
	}

	id := entity.ID

	entity.Text = "world"

	err = DBUpdate(entity)

	if err != nil {
		t.Fatal(err)
	}

	returned, err := DBGet[Entity](id)

	if err != nil {
		t.Fatal(err)
		return
	}

	if returned.Text != "world" {
		t.Fatal("updated entity but change not persisted")
	}
}

type Item struct {
	ID    int
	Value string
}

func TestInsertAll(t *testing.T) {

	DBExec("DELETE FROM Items")

	err := DBCreateTable[Item]()

	if err != nil {
		t.Fatal(err)
	}

	jobs := []*Item{
		{0, "1"},
		{0, "2"},
		{0, "3"},
	}

	err = DBInsertAll(jobs)

	if err != nil {
		t.Fatal(err)
	}

	got, err := DBGetAll[Item]()

	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 3 {
		t.Fatal("did not insert and get three items")
	}
}

type Task struct {
	AssignedTo string
}

func TestAssigningTasks(t *testing.T) {

	DBExec("DELETE FROM Tasks")

	numTasks := 10
	numWorkers := 10

	tasks := make([]Task, numTasks)

	DBCreateTable[Task]()

	DBAddAll(tasks)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := range numWorkers {
		go func() {
			workerID := fmt.Sprint(i)

			query := "UPDATE Tasks SET AssignedTo = ? WHERE rowid = (SELECT MIN(rowid) FROM Tasks WHERE AssignedTo = '')"

			err := DBExec(query, workerID)

			if err != nil {
				panic(err)
			}

			wg.Done()
		}()
	}

	wg.Wait()

	assignedTo, err := DBGetColumn[string]("SELECT AssignedTo FROM Tasks")

	if err != nil {
		t.Fatal(err)
	}

	for _, workerID := range assignedTo {
		if workerID == "" {
			t.Fatal("task not assigned to worker")
		}
	}
}

func TestDBWatcherCallbackFires(t *testing.T) {
	watcher, err := CreateDBWatcher(activeDB)

	if err != nil {
		t.Fatal(err)
	}

	callbackActivated := false

	callback := func() {
		callbackActivated = true
	}

	watcher.AddCallback(callback)

	err = DBExec("CREATE TABLE test (value ANY)")

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 20)

	if !callbackActivated {
		t.Fatal("Callback never activated")
	}
}

func TestDBWatcherHandlesData(t *testing.T) {
	watcher, err := CreateDBWatcher(activeDB)

	if err != nil {
		t.Fatal(err)
	}

	DBExec("CREATE TABLE source (value ANY)")
	DBExec("CREATE TABLE dest (value ANY)")

	callback := func() {
		data, err := DBGetSingle[string]("SELECT * FROM source")

		if err != nil {
			return
		}

		err = DBExec("INSERT INTO dest VALUES (?)", data)

		if err != nil {
			panic(err)
		}
	}

	watcher.AddCallback(callback)

	err = DBExec("INSERT INTO source VALUES (?)", "test")

	if err != nil {
		panic(err)
	}

	time.Sleep(time.Millisecond * 20)

	retValue, err := DBGetSingle[string]("SELECT * FROM dest")

	if err != nil {
		panic(err)
	}

	if retValue != "test" {
		t.Fatal("failed to move value from source to dest")
	}
}

func TestDBGetGrid(t *testing.T) {
	err := DBExec("CREATE TABLE IF NOT EXISTS gridtest (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	err = DBExec("DELETE FROM gridtest")
	if err != nil {
		t.Fatal(err)
	}

	values := []string{"A", "B", "C", "D"}
	for i, v := range values {
		err = DBExec("INSERT INTO gridtest VALUES (?, ?)", i+1, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	grid, err := DBGetGrid("SELECT * FROM gridtest")
	if err != nil {
		t.Fatal(err)
	}

	if len(grid.Rows) != len(values) {
		t.Fatalf("expected %d rows, got %d", len(values), len(grid.Rows))
	}

	for i, expectedValue := range values {
		gotValue := grid.Rows[i][1] // Second column is Value

		if expectedValue != gotValue {
			t.Fatalf("expected %v, got %v", expectedValue, gotValue)
		}
	}
}

func TestColumnMismatch(t *testing.T) {
	type Person struct {
		ID   int
		Name string
		Age  int
	}

	err := DBExec("DROP TABLE IF EXISTS Person")

	if err != nil {
		t.Fatal(err)
	}

	err = DBExec(`CREATE TABLE Persons (
		ID INTEGER PRIMARY KEY,
		Name TEXT,
		ExtraColumn TEXT,
		AnotherExtra INTEGER
	);
	
	INSERT INTO Persons VALUES (null, 'Alice', 'error', -1);
	`)

	if err != nil {
		t.Fatal(err)
	}

	people, err := DBGetAll[Person]()

	if err != nil {
		t.Fatal(err)
	}

	if len(people) != 1 {
		t.Fatal("expected 1 person, got", len(people))
	}

	person := people[0]

	if person.ID != 1 {
		t.Fatal("expected ID=1, got", person.ID)
	}

	if person.Name != "Alice" {
		t.Fatal("expected Name='Alice', got", person.Name)
	}

	if person.Age != 0 {
		t.Fatal("expected Age=0 (zero value for missing column), got", person.Age)
	}
}
