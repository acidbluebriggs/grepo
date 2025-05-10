// Package grepo provides a generic repository pattern implementation for database operations.
package grepo

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

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

	Execute(ctx context.Context, sql string, args []any) (Result, error)
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

// Execute performs the given query with args and returns a Result
func (repo repository[T]) Execute(
	ctx context.Context,
	sql string,
	args []any) (Result, error) {

	tx, err := repo.database.BeginTx(ctx, nil)

	if err != nil {
		slog.Error("unable to begin a transaction Execute() %w", "grepo", err)
		return Result{}, fmt.Errorf("function Execute() errored on Exec %w", err)

	}

	result, err := tx.Exec(sql, args...)
	if err != nil {
		_ = tx.Rollback()
		slog.Error("func Execute() errored on Exec", "grepo", err)
		return Result{}, fmt.Errorf("func Execute() errored on Exec: %w", err)
	}

	err = tx.Commit()

	if err != nil {
		slog.Error("error executing commit in Execute()", "grepo", err)
		return Result{}, fmt.Errorf("func Execute() failed during Commit: %w", err)
	}

	var lastInsertId int64
	var rowsAffected int64

	rowsAffected, rerr := result.RowsAffected()

	// There is some wonky attempts at capturing some errors here, just in case
	// one of the two result calls causes an error. We may not want to fail
	// completely. TODO need error types.
	if rerr != nil {
		slog.Error("error extracting rows affected from result", "grepo", err)
		rowsAffected = -1
	}

	lastInsertId, err = result.LastInsertId()

	if err != nil {
		slog.Error("error extracting last insert id from result", "grepo", err)
		lastInsertId = -1
		rerr = fmt.Errorf("%w", err)
	}

	r := Result{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}

	return r, rerr
}

type RowMap struct {
	m map[string]any
}

type Result struct {
	RowsAffected int64
	LastInsertId int64
}

func (m *RowMap) String(k string) string {
	switch v := m.m[k].(type) {
	case string:
		return v
	default:
		slog.Error("cannot convert %v to a string type", "grepo", v)
		return ""
	}
}

type IntegerType interface {
	~int8 | ~int16 | ~int32 | ~int64
}

// toInteger is a generic function that handles conversion to any supported integer type
func toInteger[T IntegerType](v any) T {
	switch val := v.(type) {
	case int64:
		return T(val)
	case int32:
		return T(val)
	case int16:
		return T(val)
	case int8:
		return T(val)
	default:
		slog.Error("cannot convert %v to an integer type", "grepo", val)
		return 0
	}
}

// Int64 attempts to assert and return the value within
// the RowMap with the provided key (k) as an int64.
// If the original value is an in64, the value will be
// truncated to an int64 (precision will be lost).
// If the assertion fails, then zero is returned.
func (m *RowMap) Int64(k string) int64 {
	return toInteger[int64](m.m[k])
}

// Int32 attempts to assert and return the value within
// the RowMap with the provided key (k) as an int32.
// If the original value is an in64, the value will be
// truncated to an int32 (precision will be lost).
// If the assertion fails, then zero is returned.
func (m *RowMap) Int32(k string) int32 {
	return toInteger[int32](m.m[k])
}

// Int16 attempts to assert and return the value within
// the RowMap with the provided key (k) as an int16.
// If the original value is an in64, the value will be
// truncated to an int16 (precision will be lost).
// If the assertion fails, then zero is returned.
func (m *RowMap) Int16(k string) int16 {
	return toInteger[int16](m.m[k])
}

// Int8 attempts to assert and return the value within
// the RowMap with the provided key (k) as an int8.
// If the original value is an in64, the value will be
// truncated to an int8 (precision will be lost).
// If the assertion fails, then zero is returned.
func (m *RowMap) Int8(k string) int8 {
	return toInteger[int8](m.m[k])
}

// Bool attempt to assert and return the value within
// the RowMap with the provided key (k) as a bool.
// If the assertion fails, then false is returned.
func (m *RowMap) Bool(k string) bool {
	switch v := m.m[k].(type) {
	case int64, int32, int16, int8:
		// Convert to int64 for comparison
		return v != 0
	default:
		slog.Error("attempting to call Bool resulted in a failed assertion for value", "grepo", m.m[k])
		return false
	}
}

// Bytes attempt to assert and return the value within
// the RowMap with the provided key (k) as a []byte.
// If the assertion fails, then nil is returned.
func (m *RowMap) Bytes(k string) []byte {
	r, ok := m.m[k].([]byte)
	if !ok {
		slog.Error("attempting to call Bytes resulted in a failed assertion for value %v", "grepo", m.m[k])
		return nil
	}
	return r
}
