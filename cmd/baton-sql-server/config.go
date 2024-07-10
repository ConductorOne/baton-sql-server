package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/viper"
)

var (
	dns = field.StringField("dns",
		field.WithDescription("The connection string for connecting to SQL Server"),
		field.WithRequired(true))
)

// validateConfig is run after the configuration is loaded, and should return an error if it isn't valid.
func validateConfig(_ context.Context, v *viper.Viper) error {
	if v.GetString(dns.FieldName) == "" {
		return fmt.Errorf("--dsn is required")
	}

	return nil
}
