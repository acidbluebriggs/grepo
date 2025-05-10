# Grepo - Go Repository

*Grepo* (Go Repository) is meant to be a very light SQL execution and mapping library for Go.
Yes, there a probably hundreds of these out there, but this library is currently 
being written by me to learn the Go programming language.

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

```

## Using ScanRow() / ScanRows()
ScanRow and ScanRows invokes the underlying sql.DB's Scan function. See
those docs for more info.
### ScanRow()
```go
func AlbumWithId(id int64) (*Album, error) {
  album, err := albums.ScanRow(
    context.Background(),
    "select AlbumID, Title, ArtistID from Album where AlbumId = $1",
    []any{id},
    func(scanner Scanner) (*Album, error) {
      a := &Album{}
      err := scanner.Scan(&a.AlbumID, &a.Title, &a.ArtistID)
      if err != nil {
        return nil, fmt.Errorf("error scanning %w", err)
      }
      return a, nil
    },
  )

  // of course handle the error but this is a snippet  
  return album, nil    
}
```
### ScanRows()
```go
func AllAlbums() ([]*Album, error) {
  albums, err := albums.ScanRows(
    context.Background(),
    "select AlbumID, Title, ArtistID from Album limit $1",
    nil,
    func(scanner Scanner) (*Album, error) {
      a := &Album{}
      err := scanner.Scan(&a.AlbumID, &a.Title, &a.ArtistID)
      if err != nil {
        return nil, fmt.Errorf("error scanning %w", err)
      }
      return a, nil
    },
  )
  
  // of course handle the error but this is a snippet
  return albums, nil
}  
```
