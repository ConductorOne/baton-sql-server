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
	_, _ = sb.WriteString(`
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
	l.Debug("ListServerRolePrincipals",
		zap.String("sql query", sb.String()),
		zap.Any("args", args),
	)
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
	_, _ = sb.WriteString(`
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
	l.Debug("ListServerRoles",
		zap.String("sql query", sb.String()),
		zap.Any("args", args),
	)
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
	_, _ = sb.WriteString(`
SELECT 
  principal_id, 
  sid,
  name, 
  type_desc 
FROM [`)
	_, _ = sb.WriteString(dbName)
	_, _ = sb.WriteString(`].sys.database_principals 
WHERE type = 'R' 
ORDER BY 
  principal_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY
`)
	l.Debug("ListDatabaseRoles",
		zap.String("sql query", sb.String()),
		zap.Any("args", args),
	)
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
	[%s].sys.database_principals.principal_id,
		[%s].sys.database_principals.name,
		[%s].sys.database_principals.type
		FROM 
	[%s].sys.database_principals 
	JOIN [%s].sys.database_role_members ON [%s].sys.database_role_members.member_principal_id = [%s].sys.database_principals.principal_id 
	WHERE [%s].sys.database_role_members.role_principal_id = @p1 
	ORDER BY [%s].sys.database_principals.principal_id ASC OFFSET @p2 ROWS FETCH NEXT @p3 ROWS ONLY`,
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
	l.Debug("ListDatabaseRolePrincipals",
		zap.String("sql query", query),
		zap.Any("args", args),
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

func (c *Client) GetServerRole(ctx context.Context, id string) (*RoleModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("getting database role", zap.String("id", id))

	query := `
SELECT 
  principal_id, 
  sid,
  name, 
  type_desc 
	FROM 
sys.server_principals 
WHERE type = 'R' AND principal_id = @p1
`

	var roleModel RoleModel
	row := c.db.QueryRowxContext(ctx, query, id)

	err := row.StructScan(&roleModel)
	if err != nil {
		return nil, err
	}

	return &roleModel, err
}

func (c *Client) GetDatabaseRole(ctx context.Context, dbName string, id string) (*RoleModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("getting database role", zap.String("id", id), zap.String("dbName", dbName))

	if strings.ContainsAny(dbName, "[]\"';") {
		return nil, fmt.Errorf("invalid characters in dbName")
	}

	query := `
SELECT 
  principal_id, 
  sid,
  name, 
  type_desc 
	FROM 
[%s].sys.database_principals 
WHERE type = 'R' AND principal_id = @p1
`

	query = fmt.Sprintf(
		query,
		dbName,
	)

	var roleModel RoleModel
	row := c.db.QueryRowxContext(ctx, query, id)

	err := row.StructScan(&roleModel)
	if err != nil {
		return nil, err
	}

	return &roleModel, err
}

func (c *Client) AddUserToServerRole(ctx context.Context, role string, user string) error {
	l := ctxzap.Extract(ctx)
	l.Debug("adding user to database role", zap.String("role", role), zap.String("user", user))

	if strings.ContainsAny(role, "[]\"';") || strings.ContainsAny(user, "[]\"';") {
		return fmt.Errorf("invalid characters in role or user")
	}

	query := fmt.Sprintf(`ALTER SERVER ROLE [%s] ADD MEMBER [%s];`, role, user)

	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) AddUserToDatabaseRole(ctx context.Context, role string, db string, user string) error {
	l := ctxzap.Extract(ctx)
	l.Debug("adding user to database role", zap.String("role", role), zap.String("user", user), zap.String("db", db))

	if strings.ContainsAny(role, "[]\"';") || strings.ContainsAny(user, "[]\"';") || strings.ContainsAny(db, "[]\"';") {
		return fmt.Errorf("invalid characters in role or user")
	}

	query := fmt.Sprintf(`USE [%s]; ALTER ROLE [%s] ADD MEMBER [%s];`, db, role, user)
	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) RevokeUserToServerRole(ctx context.Context, role string, user string) error {
	l := ctxzap.Extract(ctx)
	l.Debug("revoking user to database role", zap.String("role", role), zap.String("user", user))

	if strings.ContainsAny(role, "[]\"';") || strings.ContainsAny(user, "[]\"';") {
		return fmt.Errorf("invalid characters in role or user")
	}

	query := fmt.Sprintf(`ALTER SERVER ROLE [%s] DROP MEMBER [%s];`, role, user)

	l.Debug("RevokeUserToServerRole", zap.String("sql query", query))

	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) RevokeUserToDatabaseRole(ctx context.Context, role string, db string, user string) error {
	l := ctxzap.Extract(ctx)
	l.Debug("revoking user to database role", zap.String("role", role), zap.String("user", user), zap.String("db", db))

	if strings.ContainsAny(role, "[]\"';") || strings.ContainsAny(user, "[]\"';") || strings.ContainsAny(db, "[]\"';") {
		return fmt.Errorf("invalid characters in role or user")
	}

	query := fmt.Sprintf(`
USE [%s];
ALTER ROLE [%s] DROP MEMBER [%s];`, db, role, user)

	l.Debug("RevokeUserToDatabaseRole", zap.String("sql query", query))

	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}
