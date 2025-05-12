package grepo

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"path/filepath"
	"reflect"
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

func createTempDatabase(source string) (*os.File, error) {
	_, err := os.Stat(source)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to locate source database file %s: %w", source, err)
		}
		return nil, err
	}

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "temp-sqlite-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	fmt.Printf("Created temp file: %+v\n", tmpFile.Name())
	_ = tmpFile.Close()

	// Copy the original database file to the temporary file
	// we already read it, we know it's there
	input, _ := os.ReadFile(source)

	if err := os.WriteFile(tmpFile.Name(), input, 0600); err != nil {
		return nil, fmt.Errorf("failed to write temp database: %w", err)
	}

	return tmpFile, nil
}

func openDatabase(file *os.File) (*sql.DB, error) {
	// Open the temporary database
	db, err := sql.Open("sqlite3", file.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open temp database: %w", err)
	}

	return db, nil
}

func TestMain(m *testing.M) {
	_, name, _, _ := runtime.Caller(0)
	testDatabase := filepath.Join(filepath.Dir(name), "test_files", "chinook.sqlite")
	file, err := createTempDatabase(testDatabase)
	database, err := openDatabase(file)
	if err != nil {
		log.Fatal("Cannot create connection", err)
	}
	albums = NewRepository[Album](database)
	code := m.Run()
	// Cleanup
	_ = database.Close()
	_ = file.Close()
	err = os.Remove(file.Name())
	if err != nil {
		fmt.Printf("Error deleting temp test file %v", err)
	}

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

func TestExecute(t *testing.T) {

	r, err := albums.Execute(
		context.Background(),
		`insert into Artist ("name") values ($1)`,
		[]any{"Grepo"})

	if err != nil {
		t.Errorf("failed to insert row %v", err)
		return
	}

	if r.RowsAffected != 1 {
		t.Errorf("want 1 row affected got %d", r.RowsAffected)
		return
	}

	if r.LastInsertId == 0 {
		t.Errorf("want 1 row affected got %d", r.RowsAffected)
		return
	}
}

func TestNamedParameters(t *testing.T) {
	table := []struct {
		name  string
		query string
		args  map[string]any
		want  map[string]paramEntry
	}{
		{
			"1",
			"select Name from Artist where ArtistId = :artistId limit :limit",
			map[string]any{":artistId": 1, ":limit": 2},
			map[string]paramEntry{
				":artistId": {pos: 1, name: ":artistId", len: 1, val: 1},
				":limit":    {pos: 2, name: ":limit", len: 1, val: 2},
			}},
		{
			"2",
			"select Name from Artist",
			map[string]any{},
			map[string]paramEntry{},
		},
		{
			"3",
			"select Name from Artist where ArtistId in ( :ids )",
			map[string]any{":ids": []any{1, 2, 3}},
			map[string]paramEntry{
				":ids": {len: 3, pos: 1, name: ":ids", val: []any{1, 2, 3}},
			}},
	}

	for _, a := range table {
		t.Run(fmt.Sprintf("%s", a.name), func(t *testing.T) {
			t.Parallel()
			got := namedParameters(a.query, a.args)
			if !reflect.DeepEqual(a.want, got) {
				t.Errorf("want %+v got %+v", a.want, got)
			}
		})
	}
}

func TestSubstitute(t *testing.T) {
	table := []struct {
		name  string
		query string
		want  string
		m     map[string]paramEntry
	}{
		{
			"one",
			"select Name from Artist where ArtistId = :artistId limit :limit",
			"select Name from Artist where ArtistId = $1 limit $2",
			map[string]paramEntry{
				":artistId": {val: 1, name: ":artistId", len: 1, pos: 1},
				":limit":    {val: 1, name: ":limit", len: 1, pos: 2},
			},
		},
		{
			"two",
			"select Name from Artist where ArtistId in ( :ids ) limit :limit", // yes the limit is dumb, just testing replacements
			"select Name from Artist where ArtistId in ( $1, $2, $3 ) limit $4",
			map[string]paramEntry{
				":ids":   {val: []any{1, 2, 3}, name: ":ids", len: 3, pos: 1},
				":limit": {val: 1, name: ":limit", len: 1, pos: 2},
			},
		},
		{
			"three",
			"select Name from Artist\nwhere ArtistId in ( :ids )\nlimit :limit", // yes the limit is dumb, just testing replacements
			"select Name from Artist where ArtistId in ( $1, $2, $3 ) limit $4",
			map[string]paramEntry{
				":ids":   {val: []any{1, 2, 3}, name: ":ids", len: 3, pos: 1},
				":limit": {val: 1, name: ":limit", len: 1, pos: 2},
			},
		},
	}

	// note: in test 2 there is a space between the final token and the ";". It's just the way the substitution
	// works as all tokens are simply rewritten and spaced, otherwise the parsing becomes more complex and cumbersome.

	for _, a := range table {
		t.Run(fmt.Sprintf("%s", a.name), func(t *testing.T) {
			t.Parallel()
			got, err := substitute(a.query, a.m)
			if err != nil {
				t.Fatalf("failed substitution %v", err)
				return
			}
			if a.want != got {
				t.Errorf("want `%s` got `%s`", a.want, got)
			}
		})
	}

}

func TestSubstituteFails(t *testing.T) {
	s, err := substitute("select AlbumID, Title, ArtistID from Album where AlbumId = :albumId", nil)

	if err == nil {
		t.Errorf("sent zero arguments, expected one for query %s", s)
		return
	}
}

func TestRepository_MapRowN(t *testing.T) {
	results, err := albums.MapRowN(
		context.Background(),
		"select AlbumID, Title, ArtistID from Album where AlbumId = :albumId",
		map[string]any{
			":albumId": 1,
		},
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

	if results == nil {
		t.Error(fmt.Errorf("expected results"))
	}
}

func TestRepository_MapRowsN(t *testing.T) {
	results, err := albums.MapRowsN(
		context.Background(),
		"select ArtistId from Artist where ArtistId in ( :artistIds )",
		map[string]any{
			":artistIds": []int64{1, 2, 3},
		},
		func(r *RowMap) *Album {
			return &Album{
				AlbumID: r.Int64("ArtistId"),
			}
		})

	if err != nil {
		t.Error(fmt.Errorf("error retrieving rows %w", err))
		return
	}

	if len(results) != 3 {
		t.Error(fmt.Errorf("want 3 results got %d", len(results)))
	}
}
