package mssqldb

import (
	"context"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

const ServerRoleType = "server-role"
const DatabaseRoleType = "database-role"

type RoleModel struct {
	ID         int64  `db:"principal_id"`
	SecurityID string `db:"sid"`
	Name       string `db:"name"`
	Type       string `db:"type_desc"`
}

func (c *Client) ListServerRolePrincipals(ctx context.Context, pager *Pager) ([]*RoleModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing server role principals")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	// Fetch the role principals.
	// https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-server-principals-transact-sql
	sb.WriteString(`
SELECT 
  principal_id,
  sid,
  name, 
  type_desc 
FROM 
  sys.server_principals 
WHERE type = 'R'
ORDER BY 
  principal_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY
`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*RoleModel
	for rows.Next() {
		var roleModel RoleModel
		err = rows.StructScan(&roleModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &roleModel)
	}
	if rows.Err() != nil {
		return nil, "", rows.Err()
	}

	var nextPageToken string
	if len(ret) > limit {
		offset += limit
		nextPageToken = strconv.Itoa(offset)
		ret = ret[:limit]
	}

	return ret, nextPageToken, nil
}

func (c *Client) ListDatabaseRolePrincipals(ctx context.Context, dbName string, pager *Pager) ([]*RoleModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing database role principals")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	// Fetch the database role principals.
	sb.WriteString(`
SELECT 
  principal_id,
  sid,
  name, 
  type_desc 
FROM `)
	sb.WriteString(dbName)
	sb.WriteString(`.sys.database_principals 
WHERE type = 'R'
ORDER BY 
  principal_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY
`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*RoleModel
	for rows.Next() {
		var roleModel RoleModel
		err = rows.StructScan(&roleModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &roleModel)
	}
	if rows.Err() != nil {
		return nil, "", rows.Err()
	}

	var nextPageToken string
	if len(ret) > limit {
		offset += limit
		nextPageToken = strconv.Itoa(offset)
		ret = ret[:limit]
	}

	return ret, nextPageToken, nil
}
