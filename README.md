# Grepo - Go Repository

*Grepo* (Go Repository) is meant to be a very light SQL execution and mapping library for Go.
Yes, there a probably hundreds of these out there, but this library is currently 
being written by me to learn the Go programming language.

# Don't Use It

This was/is here for me to get a feel of the Go programming language. It got me to learn about:

* Go's syntax in general
* Go's abstraction concepts (interfaces, types, iterators, reflection, assertions)
* Go's built-in data structures
* Patterns in Go: singleton, factory, adapter, perhaps others
* Using a relational database using SQL within Go
* Configuration
* Error handling
* Testing, Benchmarking, Examples, and temp files
* Building and publishing

### _That being said, this ended up being a nothing-burger for usability. Waste of time and effort. 

## The Demo Database
This library is currently tested primarily around SQLite (and starting work on Postgresql), and uses the
[chinook.sqlite](https://github.com/lerocha/chinook-database/blob/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite_) demo 
database from the [Chinook Database project](https://github.com/lerocha/chinook-database).

## Using MapRows() / MapRow()

### MapRows()
MapRows executes a query and maps all rows into type []T using the provided map function.
```go
func AllAlbums() ([]*Album, error) {
  results, err := repo.MapRows(
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
  
  // of course handle the error but this is a snippet    
  return results, nil
}
```
MapRows executes a query and maps at most one row into type T using the provided map function.
### MapRow()
```go

func AlbumWithId(id int64) (*Album, error) {
  album, err := albums.MapRow(
    context.Background(),
    "select AlbumId, Title, ArtistId from Album where AlbumId = $1",
    []any{id},
    func(r *RowMap) *Album {
      return &Album{
        AlbumID:  r.Int64("AlbumId"),
        Title:    r.String("Title"),
        ArtistID: r.Int32("ArtistId"),
      }
    },
  )

    // of course handle the error but this is a snippet  
    return album, nil
}


func AlbumWith(id int64) (*Album, error) {
  album, err := albums.MapRow(
    context.Background(),
    "select AlbumId, Title, ArtistId from Album where AlbumId = $1",
    []any{id},
    func(r *RowMap) *Album {
      return &Album{
        AlbumID:  r.Int64("AlbumId"),
        Title:    r.String("Title"),
        ArtistID: r.Int32("ArtistId"),
      }
    },
  )

    // of course handle the error but this is a snippet  
    return album, nil
}

```
