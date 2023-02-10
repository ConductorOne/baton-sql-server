package mssqldb

import (
	"context"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

const TableType = "table"

type TableModel struct {
	ID   string `db:"object_id"`
	Name string `db:"name"`
	Type string `db:"type_desc"`
}

func (c *Client) ListTables(ctx context.Context, pager *Pager, dbName string) ([]*TableModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing tables")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	sb.WriteString(`SELECT object_id, name, type_desc FROM `)
	sb.WriteString(dbName)
	sb.WriteString(`.sys.tables ORDER BY object_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*TableModel
	for rows.Next() {
		var tableModel TableModel
		err = rows.StructScan(&tableModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &tableModel)
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
