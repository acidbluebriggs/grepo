package grepo

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log/slog"
	"sync"
	"time"
)

// Database is a struct which defines the configuration for connecting to a database.
// It's not very useful, as something like SQLite has a completely different
// way of creating its urls. There is no user/password/port. It's probably
// better off simply having a map of key/value and let a factory deal with it. Leaving here
// for Postgres
type Database struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Provider string `json:"provider"`
	Db       string `json:"db"`
}

type PostgresConnector struct {
	database Database
	db       *sql.DB
	mu       sync.Mutex
}

func NewPostgresConnector(database Database) *PostgresConnector {
	return &PostgresConnector{
		database: database,
	}
}

func (c *PostgresConnector) GetConnection() (*sql.DB, error) {
	c.mu.Lock()
	if c.db != nil {
		c.mu.Unlock()
		return c.db, nil
	}
	c.mu.Unlock()

	maxRetries := 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		db, err := c.tryConnect()
		if err == nil {
			c.mu.Lock()
			c.db = db
			c.mu.Unlock()
			return db, nil
		}
		lastErr = err

		slog.Warn(fmt.Sprintf("failed to connect after %d attempts: %v", maxRetries, lastErr))
		slog.Warn(fmt.Sprintf("retrying...\n"))

		backoffDuration := time.Second * time.Duration(1<<uint(i)) // exponential backoff
		time.Sleep(backoffDuration)
	}

	return nil, fmt.Errorf("failed to connect after %d attempts: %v", maxRetries, lastErr)
}

func (c *PostgresConnector) tryConnect() (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.database.Host,
		c.database.Port,
		c.database.User,
		c.database.Password,
		c.database.Db,
	)

	// Reminder, this does not n
	db, err := sql.Open(c.database.Provider, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify the connection
	if err := db.Ping(); err != nil {
		_ = db.Close() // Clean up the connection if ping fails, don't worry about this error here, we have other issues.
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

func (c *PostgresConnector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}
