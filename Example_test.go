package grepo_test

import (
	"context"
	"fmt"
	"github.com/acidbluebriggs/grepo"
	"path/filepath"
	"runtime"
)

func repo() grepo.Repository[struct{ Name string }] {
	_, name, _, _ := runtime.Caller(0)
	file := filepath.Join(filepath.Dir(name), "test_files", "chinook.sqlite")
	ct, _ := grepo.NewSQLiteConnector(file)
	conn, _ := ct.GetConnection()
	return grepo.NewRepository[struct {
		Name string
	}](conn)
}

func Example_grepo_MapRow() {
	artist, _ := repo().MapRow(
		context.Background(),
		"select Name from Artist where ArtistId = $1",
		[]any{1},
		func(r *grepo.RowMap) (*struct{ Name string }, error) {
			return &struct{ Name string }{
				Name: r.String("Name"),
			}, r.Err()
		})

	fmt.Printf("%s\n", artist.Name)

	// Output:
	// AC/DC
}

func Example_grepo_MapRows() {
	artists, _ := repo().MapRows(
		context.Background(),
		"select Name from Artist order by Name limit $1",
		[]any{3},
		func(r *grepo.RowMap) (*struct{ Name string }, error) {
			return &struct{ Name string }{
				Name: r.String("Name"),
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
	artists, _ := repo().MapRowsN(
		context.Background(),
		"select Name from Artist where ArtistId in ( :ids ) order by Name",
		map[string]any{
			"ids": []any{1, 2, 3},
		},
		func(r *grepo.RowMap) (*struct{ Name string }, error) {
			return &struct{ Name string }{
				Name: r.String("Name"),
			}, r.Err()
		})

	for _, artist := range artists {
		fmt.Printf("%s\n", artist.Name)
	}
	// Output:
	// AC/DC
	// Accept
	// Aerosmith
}
