// Package grepo provides a generic repository pattern implementation for database operations.
package grepo

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
)

// Database is a struct which defines the configuration for connecting to a database.
// It's not very useful, as something like SQLite has a completely different
// way of creating its urls. There is no user/password/port. It's probably
// better off simply having a map of key/value and let a factory deal with it.
type Database struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Provider string `json:"provider"`
	Db       string `json:"db"`
}

// Scanner defines an interface for scanning database rows into variables.
// This interface is typically implemented by database/sql.Row and database/sql.Rows.
type Scanner interface {
	// Scan copies the columns in the current row into the values pointed at by dest.
	Scan(dest ...any) error
}

type Connector interface {
	GetConnection() (*sql.DB, error)
}

// ScanFunc is a generic function type that handles scanning a database row into a specific type T.
type ScanFunc[T any] func(Scanner) (*T, error)

// MapFunc is a generic function type that converts a map of string-any pairs into a specific type T.
type MapFunc[T any] func(rowMap *RowMap) *T

// Repository defines a generic interface for database operations on type T.
type Repository[T any] interface {
	// ScanRow executes a query and scans a single row into type T using the provided scan function.
	ScanRow(ctx context.Context, sql string, args []any, scanFunc ScanFunc[T]) (*T, error)

	// ScanRows executes a query and scans multiple rows into type T using the provided scan function.
	ScanRows(ctx context.Context, sql string, args []any, scanFunc ScanFunc[T]) ([]*T, error)

	// MapRow executes a query and maps a single row into type T using the provided map function.
	MapRow(ctx context.Context, sql string, args []any, mapFunc MapFunc[T]) (*T, error)

	// MapRows executes a query and maps multiple rows into type T using the provided map function.
	MapRows(ctx context.Context, sql string, args []any, mapFunc MapFunc[T]) ([]*T, error)

	Execute(ctx context.Context, sql string, args []any) (int64, error)
}

func NewRepository[T any](db *sql.DB) Repository[T] {
	return &repository[T]{
		database: db,
	}
}

// repository is the concrete implementation of Repository interface.
type repository[T any] struct {
	// database holds the database connection
	database *sql.DB
}

func (repo repository[T]) MapRow(
	ctx context.Context,
	sql string,
	args []any,
	mapFunc MapFunc[T]) (*T, error) {

	// Yes, this function cheats and uses MapRows, which returns
	// a slice rather than a single value of T,
	// because sql.QueryRow does not allow for the ability
	// to retrieve the column names. It's not a flaw here, it's
	// a performance issue this module does not care to solve,
	// in this specific context. If you want performance without
	// the overhead of retrieving column names and creating maps,
	// then use the ScanRow/ScanRows function which are less
	// flexible.
	result, err := repo.MapRows(ctx, sql, args, mapFunc)

	if err != nil {
		// would actually log this not just return an error
		slog.Error(fmt.Sprintf("error occurred while executing row mapper: %v", err))
		return nil, fmt.Errorf("error occurred while executing row mapper: %w", err)
	}

	if len(result) > 1 {
		slog.Error("MapRow resulted in %d rows when expecting was 0 or 1", "grepo", len(result))
		return nil, fmt.Errorf("MapRow resulted in %d rows when expecting was 0 or 1", len(result))
	}

	slog.Debug("MapRow resulted in %d row(s)", "grepo", len(result))
	if len(result) == 0 {
		return nil, nil
	}

	return result[0], nil
}

func (repo repository[T]) MapRows(
	_ context.Context,
	sql string,
	args []any,
	mapFunc MapFunc[T],
) ([]*T, error) {
	stmt, err := repo.database.Prepare(sql)
	if err != nil {
		slog.Error("error preparing statement", "err", err.Error())
		return nil, err
	}

	defer func() {
		if err = stmt.Close(); err != nil {
			slog.Error("error closing statement %w", "err", err.Error())
		}
	}()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			slog.Error("error closing rows %w", "err", err)
		}
	}()

	cols, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	var results []*T
	values := make([]any, len(cols))
	ptrs := make([]any, len(values))

	for rows.Next() {
		// Take addresses directly in the Scan call
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err = rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		rowMap := toMap(cols, values)
		r := mapFunc(rowMap)

		results = append(results, r)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	slog.Debug("MapRows resulted in %d row(s)", "grepo", len(results))

	return results, nil
}

func toMap(cols []string, values []any) *RowMap {
	rowMap := make(map[string]any, len(cols))

	for i, col := range cols {
		rowMap[col] = values[i]
	}

	return &RowMap{
		m: rowMap,
	}
}

func (repo repository[T]) ScanRow(
	_ context.Context,
	sql string,
	args []any,
	scanFn ScanFunc[T]) (*T, error) {

	stmt, err := repo.database.Prepare(sql)
	if err != nil {
		slog.Error("Error preparing statement", "grepo", err.Error())
		return nil, err
	}
	defer func() {
		if err = stmt.Close(); err != nil {
			slog.Error("Error closing statement", "grepo", err.Error())
		}
	}()

	row := stmt.QueryRow(args...)
	if err = row.Err(); err != nil {
		return nil, err
	}

	scanned, err := scanFn(row)

	if err != nil {
		slog.Error("ScanRow failed %v", "grepo", err)
		return nil, err
	}

	return scanned, nil
}

func (repo repository[T]) ScanRows(
	_ context.Context,
	sql string,
	args []any,
	scanFn ScanFunc[T]) ([]*T, error) {

	stmt, err := repo.database.Prepare(sql)

	if err != nil {
		slog.Error("error preparing statement", "grepo", err)
		return nil, err
	}

	defer func() {
		if err = stmt.Close(); err != nil {
			slog.Warn("error closing rows %w", "grepo", err.Error())
		}
	}()

	rows, err := stmt.Query(args...)

	var results []*T

	for rows.Next() {
		result, err := scanFn(rows)
		if err != nil {
			slog.Error("ScanRows failed scanning row %v", "grepo", err)
			return nil, err
		}
		results = append(results, result)
	}

	slog.Debug("ScanRows resulted in %d rows(s)", "grepo", len(results))

	return results, nil
}

func (repo repository[T]) Execute(
	ctx context.Context,
	sql string,
	args []any) (int64, error) {

	tx, err := repo.database.BeginTx(ctx, nil)

	if err != nil {
		log.Fatal(err)
	}

	_, execErr := tx.Exec(sql, args...)

	if execErr != nil {
		_ = tx.Rollback()
		slog.Error("error executing stmt in Execute() %w", "grepo", err)
	}

	result, err := tx.Exec(sql, args)

	if err := tx.Commit(); err != nil {
		slog.Error("error executing commit in Execute() %w", "grepo", err)
	}

	if err != nil {
		return 0, err
	}

	rows, err := result.RowsAffected()

	if err != nil {
		return 0, err
	}

	return rows, nil
}

type RowMap struct {
	m map[string]any
}

func (m *RowMap) String(k string) string {
	return m.m[k].(string)
}

func (m *RowMap) Int64(k string) int64 {
	return m.m[k].(int64)
}

func (m *RowMap) Int32(k string) int32 {
	v64, ok := m.m[k].(int64)

	if ok {
		return int32(v64)
	}

	v32, ok := m.m[k].(int32)

	if ok {
		return v32
	}

	return 0
}

func (m *RowMap) Bool(k string) bool {
	v, ok := m.m[k].(int64)
	if ok {
		return v != 0
	}
	// Handle case where it might be stored as int32
	if v32, ok := m.m[k].(int32); ok {
		return v32 != 0
	}
	return false
}

func (m *RowMap) Bytes(k string) []byte {
	r, ok := m.m[k].([]byte)
	if !ok {
		return nil
	}
	return r
}

//func convert(v any, t string) any {
//	// just grab the first part
//	first := strings.Split(t, "(")
//	aType := first[0]
//
//	switch aType {
//	case "INTEGER", "INT":
//		return v.(int64)
//	case "CHAR", "CLOB", "TEXT", "NCHAR", "NVARCHAR", "VARCHAR", "STRING":
//		return v.(string)
//	default:
//		return v
//	}
//}
