package mssqldb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClient_GetServerPrincipalForDatabaseUser(t *testing.T) {
	t.Skip("requires a running SQL Server instance")
	ctx := context.Background()

	c, err := New(ctx, "server=127.0.0.1;user id=sa;password=devP@ssw0rd;port=1433", false)
	require.NoError(t, err)

	u, err := c.GetServerPrincipalForDatabasePrincipal(ctx, "master", 7)
	require.NoError(t, err)
	require.NotNil(t, u)

	_, err = c.GetServerPrincipalForDatabasePrincipal(ctx, "master", 77)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoServerPrincipal)
}
