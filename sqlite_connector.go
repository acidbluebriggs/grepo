package grepo

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"runtime"
)

type SQLiteConnector struct {
	path string
}

func NewSQLiteConnector(path string) (*SQLiteConnector, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to locate database file %s: %w", path, err)
		}
		return nil, err
	}
	return &SQLiteConnector{
		path,
	}, nil
}

// GetConnection currently returns a temporary copy and will be removed
// when the program terminates.
func (c *SQLiteConnector) GetConnection() (db *sql.DB, err error) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "temp-sqlite-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	tmpFile.Close()

	// Copy the original database file to the temporary file
	input, err := os.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("failed to read source database: %w", err)
	}

	if err := os.WriteFile(tmpFile.Name(), input, 0600); err != nil {
		return nil, fmt.Errorf("failed to write temp database: %w", err)
	}

	// Open the temporary database
	db, err = sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open temp database: %w", err)
	}

	// Clean up the temporary file when the database is closed
	runtime.SetFinalizer(db, func(db *sql.DB) {
		db.Close()
		os.Remove(tmpFile.Name())
	})

	return db, nil
}
