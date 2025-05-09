package grepo

import (
	"context"
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

var albums Repository[Album]

func setup() {
	_, filename, _, _ := runtime.Caller(0)
	testDataDir := filepath.Join(filepath.Dir(filename), "test_files", "chinook.sqlite")
	conn, err := NewSQLiteConnector(testDataDir)

	db, err := conn.GetConnection()

	if err != nil {
		log.Fatal("Cannot create connection", err)
	}

	albums = NewRepository[Album](db)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()

	// TODO currently the connector itself will
	// remove the temp file... we need to change this.
	//teardown()

	// Exit with the test status code
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
