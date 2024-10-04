package connector

import (
	"context"
	"os"
	"testing"

	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
	"github.com/stretchr/testify/assert"
)

var (
	dsn   = os.Getenv("BATON_DSN")
	ctx   = context.Background()
	pager = &mssqldb.Pager{
		Size:  0,
		Token: `0`,
	}
)

func TestClientListDatabasePermissions(t *testing.T) {
	tests := []struct {
		name   string
		dbName string
	}{
		{
			name:   "Checking master db",
			dbName: "master",
		},
		{
			name:   "Checking tempdb db",
			dbName: "tempdb",
		},
		{
			name:   "Checking model db",
			dbName: "model",
		},
		{
			name:   "Checking msdb db",
			dbName: "msdb",
		},
		{
			name:   "Checking tempdb db",
			dbName: "tempdb",
		},
	}

	if dsn == "" {
		t.Skip()
	}

	cli, err := mssqldb.New(ctx, dsn, false)
	assert.Nil(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pager.Token = "0"
			for keepingPagination(pager.Token) {
				_, token, err := cli.ListDatabasePermissions(ctx, test.dbName, pager)
				assert.Nil(t, err)
				pager.Token = token
			}
		})
	}
}

func TestClientListServerPermissions(t *testing.T) {
	if dsn == "" {
		t.Skip()
	}

	cli, err := mssqldb.New(ctx, dsn, false)
	assert.Nil(t, err)

	for keepingPagination(pager.Token) {
		_, token, err := cli.ListServerPermissions(ctx, pager)
		assert.Nil(t, err)
		pager.Token = token
	}
}

func TestClientListServerRoles(t *testing.T) {
	if dsn == "" {
		t.Skip()
	}

	cli, err := mssqldb.New(ctx, dsn, false)
	assert.Nil(t, err)

	for keepingPagination(pager.Token) {
		_, token, err := cli.ListServerRoles(ctx, pager)
		assert.Nil(t, err)
		pager.Token = token
	}
}

func keepingPagination(token string) bool {
	return token != ""
}

func TestClientListDatabaseRoles(t *testing.T) {
	tests := []struct {
		name   string
		dbName string
	}{
		{
			name:   "Checking master db",
			dbName: "master",
		},
		{
			name:   "Checking tempdb db",
			dbName: "tempdb",
		},
		{
			name:   "Checking model db",
			dbName: "model",
		},
		{
			name:   "Checking msdb db",
			dbName: "msdb",
		},
		{
			name:   "Checking tempdb db",
			dbName: "tempdb",
		},
	}

	if dsn == "" {
		t.Skip()
	}

	cli, err := mssqldb.New(ctx, dsn, false)
	assert.Nil(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pager.Token = "0"
			for keepingPagination(pager.Token) {
				_, token, err := cli.ListDatabaseRoles(ctx, test.dbName, pager)
				assert.Nil(t, err)
				pager.Token = token
			}
		})
	}
}

func TestClientListDatabaseRolePrincipals(t *testing.T) {
	var databaseRoleID = "16393"
	tests := []struct {
		name   string
		dbName string
	}{
		{
			name:   "Checking master db",
			dbName: "master",
		},
		{
			name:   "Checking tempdb db",
			dbName: "tempdb",
		},
		{
			name:   "Checking model db",
			dbName: "model",
		},
		{
			name:   "Checking msdb db",
			dbName: "msdb",
		},
		{
			name:   "Checking tempdb db",
			dbName: "tempdb",
		},
	}

	if dsn == "" {
		t.Skip()
	}

	cli, err := mssqldb.New(ctx, dsn, false)
	assert.Nil(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pager.Token = "0"
			for keepingPagination(pager.Token) {
				_, token, err := cli.ListDatabaseRolePrincipals(ctx, test.dbName, databaseRoleID, pager)
				assert.Nil(t, err)
				pager.Token = token
			}
		})
	}
}

func TestClientListServerRolePrincipals(t *testing.T) {
	var serverRoleID = "2"
	if dsn == "" {
		t.Skip()
	}

	cli, err := mssqldb.New(ctx, dsn, false)
	assert.Nil(t, err)

	for keepingPagination(pager.Token) {
		_, token, err := cli.ListServerRolePrincipals(ctx, serverRoleID, pager)
		assert.Nil(t, err)
		pager.Token = token
	}
}

func TestClientListDatabases(t *testing.T) {
	if dsn == "" {
		t.Skip()
	}

	cli, err := mssqldb.New(ctx, dsn, false)
	assert.Nil(t, err)

	for keepingPagination(pager.Token) {
		models, token, err := cli.ListDatabases(ctx, pager)
		assert.Nil(t, err)
		assert.NotNil(t, models)
		pager.Token = token
	}
}
