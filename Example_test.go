package grepo_test

import (
	"context"
	"fmt"
	"github.com/acidbluebriggs/grepo"
	"path/filepath"
	"runtime"
)

func repo() grepo.Repository[Artist] {
	_, name, _, _ := runtime.Caller(0)
	file := filepath.Join(filepath.Dir(name), "test_files", "chinook.sqlite")
	ct, _ := grepo.NewSQLiteConnector(file)
	conn, _ := ct.GetConnection()
	return grepo.NewRepository[Artist](conn)
}

type Artist struct {
	Name     string
	ArtistID int64
}

var NameMapper grepo.MapFunc[Artist] = func(r *grepo.RowMap) (*Artist, error) {
	return &Artist{
		Name:     r.String("Name"),
		ArtistID: r.Int64("ArtistId"),
	}, r.Err()
}

func Example_grepo_MapRow() {

	// ignoring error for the example as it "works"
	artist, _ := repo().MapRow(
		context.Background(),
		"select Name, ArtistId from Artist where ArtistId = $1",
		[]any{1},
		// can pass a function as the mapper
		NameMapper,
	)

	fmt.Printf("%s\n", artist.Name)

	// Output:
	// AC/DC
}

func Example_grepo_MapRows() {
	// ignoring error for the example
	artists, _ := repo().MapRows(
		context.Background(),
		"select Name, ArtistId from Artist order by Name limit $1",
		[]any{3},
		// with inline mapping function
		func(r *grepo.RowMap) (*Artist, error) {
			return &Artist{
				Name:     r.String("Name"),
				ArtistID: r.Int64("ArtistId"),
			}, r.Err()
		})

	for _, artist := range artists {
		fmt.Printf("%s\n", artist.Name)
	}

	// Output:
	// A Cor Do Som
	// AC/DC
	// Aaron Copland & London Symphony Orchestra
}

func Example_grepo_MapRowsN() {
	// ignoring error for the example
	artists, _ := repo().MapRowsN(
		context.Background(),
		"select Name, ArtistId from Artist where ArtistId in ( :ids ) order by Name",
		map[string]any{
			"ids": []any{1, 2, 3},
		},
		NameMapper,
	)

	for _, artist := range artists {
		fmt.Printf("%s\n", artist.Name)
	}
	// Output:
	// AC/DC
	// Accept
	// Aerosmith
}
