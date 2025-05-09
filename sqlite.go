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
func (c *SQLiteConnector) GetConnection() (*sql.DB, error) {
	// Open the temporary database
	db, err := sql.Open("sqlite3", c.path)

	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	// Clean up the temporary file when the database is closed
	runtime.SetFinalizer(db, func(db *sql.DB) {
		_ = db.Close()
	})

	return db, nil
}
