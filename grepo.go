// Package grepo provides a generic repository pattern implementation for database operations.
package grepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"unicode"
)

type Connector interface {
	GetConnection() (*sql.DB, error)
}

// MapFunc is a generic function type that converts a map of string-any pairs into a specific type T.
type MapFunc[T any] func(r *RowMap) (*T, error)
type ApplyFunc[T any] func(t *T, r *RowMap) (*T, error)

// Repository defines a generic interface for database operations on type T.
type Repository[T any] interface {
	// MapRow executes a query and maps a single row into type T using the provided map function.
	MapRow(ctx context.Context, sql string, args []any, mapFunc MapFunc[T]) (*T, error)

	// MapRowN executes a query and maps a single row into type T using the provided map function.
	MapRowN(ctx context.Context, sql string, args map[string]any, mapFunc MapFunc[T]) (*T, error)

	// MapRows executes a query and maps multiple rows into type T using the provided map function.
	MapRows(ctx context.Context, sql string, args []any, mapFunc MapFunc[T]) ([]*T, error)

	// MapRowsN executes a query and maps multiple rows into type T using the provided map function.
	MapRowsN(ctx context.Context, sql string, args map[string]any, mapFunc MapFunc[T]) ([]*T, error)

	// Execute experimental update, does not support slices yet.
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
		return nil, errors.Join(errors.New("error occurred while executing row mapper"), err)
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

func (repo repository[T]) MapRowN(
	ctx context.Context,
	sql string,
	args map[string]any,
	mapFunc MapFunc[T]) (*T, error) {

	entries := namedParameters(sql, args)
	query, err := substitute(sql, entries)
	newArgs := flattenArgs(entries)

	if err != nil {
		return nil, fmt.Errorf("substitution of named parameters failed %w", err)
	}

	result, err := repo.MapRow(ctx, query, newArgs, mapFunc)

	if err != nil {
		slog.Error(fmt.Sprintf("unable to execute query '%s' with parameters %v", query, args))
		return nil, err
	}

	return result, nil
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

	// need to handle the issue if we have slice in the args (like an IN clause arg)
	// The point here is that were are going to expand all arguments to their positions in the
	// the statement.
	// Is reflection the correct thing? Type assertions were ugly, but perhaps a better way? not sure.
	for i, arg := range args {
		switch v := arg.(type) {
		default:
			// Check if it's any kind of slice
			rv := reflect.ValueOf(v)
			if rv.Kind() == reflect.Slice {
				replacements := make([]string, rv.Len())
				if rv.IsValid() && !rv.IsNil() {
					for i := 0; i < rv.Len(); i++ {
						elem := rv.Index(i)
						switch elem.Kind() {
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
							replacements[i] = fmt.Sprintf("%d", elem.Int())
						case reflect.Bool:
							replacements[i] = fmt.Sprintf("%t", elem.Bool())
						case reflect.String:
							replacements[i] = fmt.Sprintf("'%s'", elem.String())
						case reflect.Float32, reflect.Float64:
							replacements[i] = fmt.Sprintf("%f", elem.Float())
						default:
							replacements[i] = fmt.Sprintf("%v", elem.Interface())
						}
					}
				}
				args[i] = strings.Join(replacements, ", ")
			}
		}
	}
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

		r, err := mapFunc(rowMap)
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	slog.Debug("MapRows resulted in %d row(s)", "grepo", len(results))

	return results, nil
}

func (repo repository[T]) MapRowsN(
	ctx context.Context,
	sql string,
	args map[string]any,
	mapFunc MapFunc[T]) ([]*T, error) {

	entries := namedParameters(sql, args)
	query, err := substitute(sql, entries)

	if err != nil {
		return nil, fmt.Errorf("substitution of named parameters failed %w", err)
	}

	newArgs := flattenArgs(entries)
	result, err := repo.MapRows(ctx, query, newArgs, mapFunc)

	if err != nil {
		slog.Error(fmt.Sprintf("unable to execute query '%s' with parameters %v", query, args))
		return nil, err
	}

	return result, nil
}

func flattenArgs(entries map[string]paramEntry) []any {
	// need the entries sorted by their position
	sorted := slices.SortedFunc(maps.Values(entries), func(entry paramEntry, entry2 paramEntry) int {
		return entry.pos - entry2.pos
	})

	var newArgs []any

	for _, pe := range sorted {
		switch v := pe.val.(type) {
		default:
			// Check if it's any kind of slice
			rv := reflect.ValueOf(v)
			if rv.Kind() == reflect.Slice {
				if rv.IsValid() && !rv.IsNil() {
					for i := 0; i < rv.Len(); i++ {
						elem := rv.Index(i)
						if elem.IsValid() {
							newArgs = append(newArgs, elem.Interface())
						}
					}
					pe.len = rv.Len()
				}
			} else {
				newArgs = append(newArgs, pe.val)
			}
		}
	}

	return newArgs
}

// Execute performs the given query with args and returns a Result
func (repo repository[T]) Execute(
	ctx context.Context,
	sql string,
	args []any) (Result, error) {

	tx, err := repo.database.BeginTx(ctx, nil)

	if err != nil {
		slog.Error(fmt.Sprintf("unable to begin a transaction Execute() %v", err))
		return Result{}, fmt.Errorf("function Execute() errored on Exec %w", err)

	}

	result, err := tx.Exec(sql, args...)
	if err != nil {
		_ = tx.Rollback()
		slog.Error(fmt.Sprintf("func Execute() errored on Exec %v", err))
		return Result{}, fmt.Errorf("func Execute() errored on Exec: %w", err)
	}

	err = tx.Commit()

	if err != nil {
		slog.Error(fmt.Sprintf("error executing commit in Execute()  %v", err))
		return Result{}, fmt.Errorf("func Execute() failed during Commit: %w", err)
	}

	var lastInsertId int64
	var rowsAffected int64

	rowsAffected, rerr := result.RowsAffected()

	// There is some wonky attempts at capturing some errors here, just in case
	// one of the two result calls causes an error. We may not want to fail
	// completely. TODO need error types.
	if rerr != nil {
		slog.Error(fmt.Sprintf("error extracting rows affected from result %v", err))
		rowsAffected = -1
	}

	lastInsertId, err = result.LastInsertId()

	if err != nil {
		slog.Error(fmt.Sprintf("error extracting last insert id from result %v", err))
		lastInsertId = -1
		rerr = fmt.Errorf("%w", err)
	}

	r := Result{
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
	}

	return r, rerr
}

type IntegerType interface {
	~int8 | ~int16 | ~int32 | ~int64
}

type FloatType interface {
	~float64 | ~float32
}

type RowMap struct {
	m map[string]any
	// Errors is for the convenience of collecting any errors during
	// the reading of a RowMap's convenience functions such as
	// the Int* functions, String(), Bool, Bytes. These functions
	// will store any errors that might have been encountered during
	// these calls.
	// TODO, explain more... it's so you don't have to keep checking errors
	// durning the mapping process, just return: if r.errors != nil { }
	errors []error
}

type Result struct {
	RowsAffected int64
	LastInsertId int64
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

// toInteger is a generic function that handles conversion to any supported integer type
func toInteger[T IntegerType](v any) (T, error) {
	switch val := v.(type) {
	case int64:
		return T(val), nil
	case int32:
		return T(val), nil
	case int16:
		return T(val), nil
	case int8:
		return T(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %v to an integer type", v)
	}
}

func toFloat[T FloatType](v any) (T, error) {
	switch val := v.(type) {
	case float32:
		return T(val), nil
	case float64:
		return T(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %v to an float type", v)
	}
}

func (m *RowMap) addErr(e error) {
	m.errors = append(m.errors, e)
}

func (m *RowMap) Err() error {
	return errors.Join(m.errors...)
}

func (m *RowMap) Apply(t *any) (*any, error) {
	if err := m.Err(); m != nil {
		return nil, err
	}
	return t, nil
}

func (m *RowMap) try(k string) error {
	if _, ok := m.m[k]; !ok {
		return fmt.Errorf("key '%s' does not exist in row map", k)
	}
	return nil
}

func (c ColReadError) Error() string {
	return fmt.Sprintf("cannot convert key '%s' value '%v' to '%s'", c.key, c.value, c.target)
}

func NewColReadError(key string, value any, target string) ColReadError {
	return ColReadError{
		key, value, target,
	}
}

func (m *RowMap) String(k string) string {
	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return ""
	}

	switch v := m.m[k].(type) {
	case string:
		return v
	default:
		m.addErr(NewColReadError(k, v, "string"))
		return ""
	}
}

// Int64 attempts to assert and return the value within
// the RowMap with the provided key (k) as an int64.
// If the original value is an in64, the value will be
// truncated to an int64 (precision will be lost).
// If the assertion fails, then zero is returned.
func (m *RowMap) Int64(k string) int64 {

	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return 0
	}

	if v, err := toInteger[int64](m.m[k]); err != nil {
		m.addErr(NewColReadError(k, v, "int64"))
		return 0
	} else {
		return v
	}
}

// Int32 attempts to assert and return the value within
// the RowMap with the provided key (k) as an int32.
// If the original value is an in64, the value will be
// truncated to an int32 (precision will be lost).
// If the assertion fails, then zero is returned.
func (m *RowMap) Int32(k string) int32 {
	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return 0
	}

	if v, err := toInteger[int32](m.m[k]); err != nil {
		m.addErr(NewColReadError(k, v, "int32"))
		return 0
	} else {
		return v
	}
}

// Int16 attempts to assert and return the value within
// the RowMap with the provided key (k) as an int16.
// If the original value is an in64, the value will be
// truncated to an int16 (precision will be lost).
// If the assertion fails, then zero is returned.
func (m *RowMap) Int16(k string) int16 {
	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return 0
	}

	if v, err := toInteger[int16](m.m[k]); err != nil {
		m.addErr(NewColReadError(k, v, "int16"))
		return 0
	} else {
		return v
	}
}

// Int8 attempts to assert and return the value within
// the RowMap with the provided key (k) as an int8.
// If the original value is anything larger, the value will be
// truncated to an int8 (precision will be lost).
// If the assertion fails, then zero is returned.
func (m *RowMap) Int8(k string) int8 {
	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return 0
	}

	if v, err := toInteger[int8](m.m[k]); err != nil {
		m.addErr(NewColReadError(k, v, "int8"))
		return 0
	} else {
		return v
	}
}

// Float64 attempts to assert and return the value within
// the RowMap with the provided key (k) as a float64.
// If the assertion fails, then zero is returned.
func (m *RowMap) Float64(k string) float64 {
	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return 0
	}

	if v, err := toFloat[float64](m.m[k]); err != nil {
		m.addErr(NewColReadError(k, v, "float64"))
		return 0
	} else {
		return v
	}
}

// Float32 attempts to assert and return the value within
// the RowMap with the provided key (k) as a float64.
// If the assertion fails, then zero is returned.
func (m *RowMap) Float32(k string) float32 {
	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return 0
	}

	if v, err := toFloat[float32](m.m[k]); err != nil {
		m.addErr(NewColReadError(k, v, "float32"))
		return 0
	} else {
		return v
	}
}

// Bool attempt to assert and return the value within
// the RowMap with the provided key (k) as a bool.
// If the assertion fails, then false is returned.
func (m *RowMap) Bool(k string) bool {
	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return false
	}

	switch v := m.m[k].(type) {
	case int64, int32, int16, int8:
		// Convert to int64 for comparison
		return v == 0
	default:
		m.addErr(NewColReadError(k, v, "bool"))
		return false
	}
}

// Bytes attempt to assert and return the value within
// the RowMap with the provided key (k) as a []byte.
// If the assertion fails, then nil is returned.
func (m *RowMap) Bytes(k string) []byte {
	err := m.try(k)

	if err != nil {
		m.addErr(err)
		return nil
	}

	r, ok := m.m[k].([]byte)
	if !ok {
		m.addErr(NewColReadError(k, r, "[]byte"))
		return nil
	}
	return r
}

type ColReadError struct {
	key    string
	value  any
	target string
}

type paramEntry struct {
	name string //probably redundant as it will be used in a map... maybe
	pos  int
	val  any
	len  int
}

func namedParameters(s string, args map[string]any) map[string]paramEntry {
	params := make(map[string]paramEntry)
	fields := strings.Fields(s)
	position := 0

	for _, word := range fields {
		if strings.HasPrefix(word, ":") {
			position++
			param := strings.TrimFunc(word, func(r rune) bool {
				return !unicode.IsLetter(r) && !unicode.IsNumber(r) && (r == ':' || r == '(' || r == ')')
			})

			pe := paramEntry{
				pos:  position,
				name: param,
				val:  args[param],
				len:  1,
			}

			switch v := args[param].(type) {
			default:
				// Check if it's any kind of slice
				rv := reflect.ValueOf(v)
				if rv.Kind() == reflect.Slice {
					pe.len = rv.Len()
					position += rv.Len()
				}
			}

			params[param] = pe
		}
	}

	// this needs to error if the named param is not found
	return params
}

func substitute(sql string, params map[string]paramEntry) (string, error) {
	fields := strings.Fields(sql)
	var found []string
	position := 1

	for i, word := range fields {
		if strings.HasPrefix(word, ":") {
			param := strings.TrimFunc(word, func(r rune) bool {
				return !unicode.IsLetter(r) && !unicode.IsNumber(r) && (r == ':' || r == '(' || r == ')')
			})
			found = append(found, param)

			if pe, exists := params[param]; exists {
				positions := make([]string, pe.len)
				for pi := range pe.len {
					positions[pi] = fmt.Sprintf("$%d", position)
					position++
				}
				fields[i] = strings.Join(positions, ", ")
			} else {
				// could error and show where in the token/field path it failed
				return "", fmt.Errorf("parameter %s not found in args %v", colorize(param, Red), params)
			}
		}
	}

	if len(found) != len(params) {
		return "", fmt.Errorf("received %d arguments and only replaced %d", len(params), len(found))
	}

	return strings.Join(fields, " "), nil
}
