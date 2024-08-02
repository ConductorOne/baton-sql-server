package mssqldb

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

const (
	UserType         = "user"
	DatabaseUserType = "database-user"
)

var ErrNoServerPrincipal = errors.New("no server principal found")

type UserModel struct {
	ID         string `db:"principal_id"`
	SecurityID string `db:"sid"`
	Name       string `db:"name"`
	Type       string `db:"type_desc"`
	IsDisabled bool   `db:"is_disabled"`
}

func (c *Client) ListServerUserPrincipals(ctx context.Context, pager *Pager) ([]*UserModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing user principals")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	// Fetch the user principals.
	// https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-server-principals-transact-sql
	_, _ = sb.WriteString(`
SELECT 
  principal_id,
  sid,
  name, 
  type_desc,
  is_disabled
FROM 
  sys.server_principals
WHERE 
  (
    type = 'S' 
    OR type = 'U' 
    OR type = 'C' 
    or type = 'E' 
    or type = 'K'
  ) 
ORDER BY 
  principal_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY
`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*UserModel
	for rows.Next() {
		var userModel UserModel
		err = rows.StructScan(&userModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &userModel)
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

// GetServerPrincipalForDatabasePrincipal returns the server principal for a given database user.
// Returns ErrNoServerPrincipal if no server principal is found.
func (c *Client) GetServerPrincipalForDatabasePrincipal(ctx context.Context, dbName string, principalID int64) (*UserModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("getting server principal for database user")

	var sb strings.Builder
	_, _ = sb.WriteString(`
SELECT
	principal_id,
	sid,
	name,
	type_desc,
	is_disabled
FROM
    sys.server_principals 
WHERE sid = (SELECT sid FROM [`)
	_, _ = sb.WriteString(dbName)
	_, _ = sb.WriteString(`].sys.database_principals WHERE principal_id = @p1)`)

	row := c.db.QueryRowxContext(ctx, sb.String(), principalID)
	if row.Err() != nil {
		return nil, row.Err()
	}

	var ret UserModel
	err := row.StructScan(&ret)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoServerPrincipal
		}
		return nil, err
	}

	return &ret, nil
}

func (c *Client) ListDatabaseUserPrincipals(ctx context.Context, dbName string, pager *Pager) ([]*UserModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing database user principals")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	_, _ = sb.WriteString(`
SELECT 
  principal_id,
  name, 
  type_desc
FROM [`)
	_, _ = sb.WriteString(dbName)
	_, _ = sb.WriteString(`].sys.database_principals
WHERE 
  (
    type = 'S' 
    OR type = 'U' 
    OR type = 'C' 
    or type = 'E' 
    or type = 'K'
  ) 
ORDER BY 
  principal_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY
`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*UserModel
	for rows.Next() {
		var userModel UserModel
		err = rows.StructScan(&userModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &userModel)
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
