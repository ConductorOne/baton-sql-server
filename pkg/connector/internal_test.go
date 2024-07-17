package connector

import (
	"context"
	"os"
	"testing"

	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
	"github.com/stretchr/testify/assert"
)

var (
	dsn = os.Getenv("BATON_DSN")
)

func TestClientListDatabasePermissions(t *testing.T) {
	var (
		ctx    = context.Background()
		dbName = "temp_db"
		pager  = &mssqldb.Pager{
			Size:  0,
			Token: `{"states":null,"current_state":{"token":"1","resource_type_id":"database_user","resource_id":""}}`,
		}
	)

	if dsn == "" {
		t.Skip()
	}

	cli, err := mssqldb.New(ctx, dsn)
	assert.Nil(t, err)

	for pager.Token != "" {
		pm, token, err := cli.ListDatabasePermissions(ctx, dbName, pager)
		assert.Nil(t, err)
		assert.NotNil(t, pm)
		assert.NotNil(t, token)
	}
}
