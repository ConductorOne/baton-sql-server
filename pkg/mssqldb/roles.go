package mssqldb

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const ServerRoleType = "server-role"
const DatabaseRoleType = "database-role"

type RoleModel struct {
	ID         int64  `db:"principal_id"`
	SecurityID string `db:"sid"`
	Name       string `db:"name"`
	Type       string `db:"type_desc"`
}

type RolePrincipalModel struct {
	ID   int64  `db:"principal_id"`
	Name string `db:"name"`
	Type string `db:"type"`
}

func (c *Client) ListServerRolePrincipals(ctx context.Context, serverRoleID string, pager *Pager) ([]*RolePrincipalModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing server role members")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{serverRoleID, offset, limit + 1}

	var sb strings.Builder
	// Fetch the role principals.
	// https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-server-principals-transact-sql
	sb.WriteString(`
SELECT 
  sys.server_principals.principal_id,
  sys.server_principals.name, 
  sys.server_principals.type
FROM 
  sys.server_principals
JOIN sys.server_role_members ON sys.server_role_members.member_principal_id = sys.server_principals.principal_id
WHERE sys.server_role_members.role_principal_id = @p1
ORDER BY 
  sys.server_principals.principal_id ASC OFFSET @p2 ROWS FETCH NEXT @p3 ROWS ONLY
`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*RolePrincipalModel
	for rows.Next() {
		var rolePrincipalModel RolePrincipalModel
		err = rows.StructScan(&rolePrincipalModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &rolePrincipalModel)
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

func (c *Client) ListServerRoles(ctx context.Context, pager *Pager) ([]*RoleModel, string, error) {
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

func (c *Client) ListDatabaseRoles(ctx context.Context, dbName string, pager *Pager) ([]*RoleModel, string, error) {
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

func (c *Client) ListDatabaseRolePrincipals(ctx context.Context, dbName string, databaseRoleID string, pager *Pager) ([]*RolePrincipalModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing database role members", zap.String("database_role_id", databaseRoleID), zap.String("database_name", dbName))

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{databaseRoleID, offset, limit + 1}

	query := fmt.Sprintf(
		`SELECT
	%s.sys.database_principals.principal_id,
		%s.sys.database_principals.name,
		%s.sys.database_principals.type
		FROM
	%s.sys.database_principals
	JOIN %s.sys.database_role_members ON %s.sys.database_role_members.member_principal_id = %s.sys.database_principals.principal_id
	WHERE %s.sys.database_role_members.role_principal_id = @p1
	ORDER BY %s.sys.database_principals.principal_id ASC OFFSET @p2 ROWS FETCH NEXT @p3 ROWS ONLY`,
		dbName,
		dbName,
		dbName,
		dbName,
		dbName,
		dbName,
		dbName,
		dbName,
		dbName,
	)

	rows, err := c.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*RolePrincipalModel
	for rows.Next() {
		var rolePrincipalModel RolePrincipalModel
		err = rows.StructScan(&rolePrincipalModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &rolePrincipalModel)
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
