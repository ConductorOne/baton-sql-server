package mssqldb

import (
	"context"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

type PermissionModel struct {
	PrincipalName string `db:"principal_name"`
	PrincipalID   int64  `db:"principal_id"`
	PrincipalType string `db:"principal_type"`
	State         string `db:"state"`
	Permissions   string `db:"perms"`
}

func (c *Client) ListServerPermissions(ctx context.Context, pager *Pager) ([]*PermissionModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing server permissions")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	_, _ = sb.WriteString(`SELECT 
principals.name as principal_name, 
perms.grantee_principal_id as principal_id, 
perms.state as state, 
STRING_AGG(perms.type, ',') as perms, 
principals.type as principal_type 
FROM sys.server_permissions perms 
         JOIN sys.server_principals principals ON perms.grantee_principal_id = principals.principal_id 
WHERE perms.state = 'G' OR perms.state = 'W' 
GROUP BY perms.grantee_principal_id, perms.state, principals.name, principals.type 
ORDER BY perms.grantee_principal_id ASC 
OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*PermissionModel
	for rows.Next() {
		var spModel PermissionModel
		err = rows.StructScan(&spModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &spModel)
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

func (c *Client) ListDatabasePermissions(ctx context.Context, dbName string, pager *Pager) ([]*PermissionModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing database permissions")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	_, _ = sb.WriteString(`SELECT
    principals.name as principal_name,
    perms.grantee_principal_id as principal_id,
    perms.state as state,
    STRING_AGG(perms.type, ',') as perms,
    principals.type as principal_type
FROM `)
	_, _ = sb.WriteString(dbName)
	_, _ = sb.WriteString(`.sys.database_permissions perms
         JOIN `)
	_, _ = sb.WriteString(dbName)
	_, _ = sb.WriteString(`.sys.database_principals AS principals 
             ON perms.grantee_principal_id = principals.principal_id 
WHERE (perms.state = 'G' OR perms.state = 'W') AND (perms.class = 0 AND perms.major_id = 0) 
GROUP BY perms.grantee_principal_id, perms.state, principals.name, principals.type 
ORDER BY perms.grantee_principal_id ASC 
OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*PermissionModel
	for rows.Next() {
		var dpModel PermissionModel
		err = rows.StructScan(&dpModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &dpModel)
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
