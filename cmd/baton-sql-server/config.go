package main

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	dsn = field.StringField("dsn",
		field.WithDescription("The connection string for connecting to SQL Server"),
		field.WithRequired(true))
	skipUnavailableDatabases = field.BoolField("skip-unavailable-databases",
		field.WithDescription("Skip databases that are unavailable (offline, restoring, etc)"))
)

var cfg = field.Configuration{
	Fields: []field.SchemaField{dsn, skipUnavailableDatabases},
}
