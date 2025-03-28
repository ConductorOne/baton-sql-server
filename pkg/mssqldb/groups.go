package mssqldb

import (
	"context"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

const GroupType = "group"

type GroupModel struct {
	ID         string `db:"principal_id"`
	SecurityID string `db:"sid"`
	Name       string `db:"name"`
	Type       string `db:"type_desc"`
}

func (c *Client) ListGroupPrincipals(ctx context.Context, pager *Pager) ([]*GroupModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing group principals")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	// Fetch the group principals.
	// https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-server-principals-transact-sql
	_, _ = sb.WriteString(`
SELECT 
  principal_id, 
  sid,
  name, 
  type_desc 
FROM 
  sys.server_principals
WHERE 
  (
    type = 'G' 
    OR type = 'X'
  ) 
ORDER BY 
  principal_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY
`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*GroupModel
	for rows.Next() {
		var groupModel GroupModel
		err = rows.StructScan(&groupModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &groupModel)
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
