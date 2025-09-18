package dbdt

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
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
