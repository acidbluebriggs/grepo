package grepo

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestSQLiteConnector(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testDatabase := filepath.Join(filepath.Dir(filename), "test_files", "chinook.sqlite")

	c, err := NewSQLiteConnector(testDatabase)

	if err != nil {
		t.Fatalf("connector failed %v", err)
	}

	conn, err := c.GetConnection()

	if err != nil {
		t.Fatalf("connector failed to open the database %v", err)
	}

	err = conn.Ping()

	if err != nil {
		t.Fatalf("connetor failed to ping the database %v", err)
	}

}
