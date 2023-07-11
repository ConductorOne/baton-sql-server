package mssqldb

import (
	"context"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const DatabaseType = "database"

type DbModel struct {
	ID   int64  `db:"database_id"`
	Name string `db:"name"`
}

func (c *Client) GetDatabase(ctx context.Context, id int64) (*DbModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("fetching database", zap.Int64("database_id", id))

	var sb strings.Builder
	_, _ = sb.WriteString(`SELECT name, database_id FROM sys.databases WHERE database_id=@p1`)

	row := c.db.QueryRowxContext(ctx, sb.String(), id)
	if row.Err() != nil {
		return nil, row.Err()
	}

	var ret DbModel
	err := row.StructScan(&ret)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}

func (c *Client) ListDatabases(ctx context.Context, pager *Pager) ([]*DbModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing databases")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	_, _ = sb.WriteString(`SELECT name, database_id FROM sys.databases
                                      ORDER BY database_id ASC 
                                      OFFSET @p1 ROWS
                                      FETCH NEXT @p2 ROWS ONLY`)

	l.Debug("SQL QUERY", zap.String("q", sb.String()))

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*DbModel
	for rows.Next() {
		var dbModel DbModel
		err = rows.StructScan(&dbModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &dbModel)
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
