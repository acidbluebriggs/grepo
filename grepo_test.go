package grepo

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type Album struct {
	ArtistID int32
	AlbumID  int64
	Title    string
}

var (
	albums Repository[Album]
)

func openTempDb(path string) (*sql.DB, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to locate database file %s: %w", path, err)
		}
		return nil, err
	}

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "temp-sqlite-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	_ = tmpFile.Close()

	// Copy the original database file to the temporary file
	input, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read source database: %w", err)
	}

	if err := os.WriteFile(tmpFile.Name(), input, 0600); err != nil {
		return nil, fmt.Errorf("failed to write temp database: %w", err)
	}

	// Open the temporary database
	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open temp database: %w", err)
	}

	runtime.SetFinalizer(db, func(db *sql.DB) {
		_ = db.Close()
		_ = os.Remove(tmpFile.Name())
	})

	return db, nil
}

func setup() {
	_, name, _, _ := runtime.Caller(0)
	testDatabase := filepath.Join(filepath.Dir(name), "test_files", "chinook.sqlite")
	database, err := openTempDb(testDatabase)
	if err != nil {
		log.Fatal("Cannot create connection", err)
	}
	albums = NewRepository[Album](database)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

func TestMapRows(t *testing.T) {
	results, err := albums.MapRows(
		context.Background(),
		"select AlbumId, Title, ArtistId from Album",
		nil,
		func(r *RowMap) *Album {
			return &Album{
				AlbumID:  r.Int64("AlbumId"),
				Title:    r.String("Title"),
				ArtistID: r.Int32("ArtistId"),
			}
		})

	if err != nil {
		t.Error(fmt.Errorf("error retrieving rows %w", err))
		return
	}

	if len(results) == 0 {
		t.Error(fmt.Errorf("expected results"))
	}
}

func TestMapRow(t *testing.T) {
	album, err := albums.MapRow(
		context.Background(),
		"select AlbumId, Title, ArtistId from Album where AlbumId = $1",
		[]any{1},
		func(r *RowMap) *Album {
			return &Album{
				AlbumID:  r.Int64("AlbumId"),
				Title:    r.String("Title"),
				ArtistID: r.Int32("ArtistId"),
			}
		},
	)

	if err != nil {
		t.Error(fmt.Errorf("error retrieving rows %w", err))
		return
	}

	if album == nil {
		t.Error(fmt.Errorf("want album with AlbumId 1, got nil"))
	}
}

func TestScanRow(t *testing.T) {

	album, err := albums.ScanRow(
		context.Background(),
		"select AlbumID, Title, ArtistID from Album where AlbumId = $1",
		[]any{1},
		func(scanner Scanner) (*Album, error) {
			a := &Album{}

			err := scanner.Scan(&a.AlbumID, &a.Title, &a.ArtistID)

			if err != nil {
				return nil, fmt.Errorf("error scanning %w", err)
			}

			return a, nil
		},
	)

	if err != nil {
		t.Error(fmt.Errorf("error retrieving rows %w", err))
		return
	}

	if album == nil {
		t.Error(fmt.Errorf("want album with AlbumId 1, got nil"))
	}
}

func TestScanRows(t *testing.T) {

	album, err := albums.ScanRows(
		context.Background(),
		"select AlbumID, Title, ArtistID from Album limit $1",
		[]any{10},
		func(scanner Scanner) (*Album, error) {
			a := &Album{}

			err := scanner.Scan(&a.AlbumID, &a.Title, &a.ArtistID)

			if err != nil {
				return nil, fmt.Errorf("error scanning %w", err)
			}

			return a, nil
		},
	)

	if err != nil {
		t.Errorf("error retrieving rows %v", err)
		return
	}

	if album == nil {
		t.Errorf("want album with AlbumId 1, got nil")
	}

	if len(album) != 10 {
		t.Errorf("want 10 albums got %d", len(album))
	}
}
