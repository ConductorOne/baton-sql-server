package mssqldb

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

const SchemaType = "schema"

type SchemaModel struct {
	ID      int64  `db:"schema_id"`
	OwnerID int64  `db:"principal_id"`
	Name    string `db:"name"`
}

func (c *Client) ListSchemas(ctx context.Context, pager *Pager, dbName string) ([]*SchemaModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing schemas")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	sb.WriteString(`SELECT schema_id, principal_id, name FROM `)
	sb.WriteString(dbName)
	sb.WriteString(`.sys.schemas ORDER BY schema_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY`)

	fmt.Println(sb.String())

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*SchemaModel
	for rows.Next() {
		var schemaModel SchemaModel
		err = rows.StructScan(&schemaModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &schemaModel)
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
