package mssqldb

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
)

type Client struct {
	db                       *sqlx.DB
	skipUnavailableDatabases bool
}

// List databases
// SELECT name, database_id, create_date FROM sys.databases;

// List tables
// SELECT * FROM master.INFORMATION_SCHEMA.TABLES;

// List users

func New(ctx context.Context, dsn string, skipUnavailableDatabases bool) (*Client, error) {
	db, err := sqlx.Connect("sqlserver", dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 1)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	c := &Client{
		db:                       db,
		skipUnavailableDatabases: skipUnavailableDatabases,
	}

	return c, nil
}
