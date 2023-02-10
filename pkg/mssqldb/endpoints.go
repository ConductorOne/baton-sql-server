package mssqldb

import (
	"context"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const EndpointType = "endpoint"

type EndpointModel struct {
	ID   int64  `db:"endpoint_id"`
	Name string `db:"name"`
}

func (c *Client) GetEndpoint(ctx context.Context, id int64) (*EndpointModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("fetching endpoint", zap.Int64("endpoint_id", id))

	var sb strings.Builder
	sb.WriteString(`SELECT name, endpoint_id FROM sys.endpoints WHERE endpoint_id=@p1`)

	row := c.db.QueryRowxContext(ctx, sb.String(), id)
	if row.Err() != nil {
		return nil, row.Err()
	}

	var ret EndpointModel
	err := row.StructScan(&ret)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}

func (c *Client) ListEndpoints(ctx context.Context, pager *Pager) ([]*EndpointModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing endpoints")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	sb.WriteString(`SELECT name, endpoint_id FROM sys.endpoints
                                      ORDER BY endpoint_id ASC 
                                      OFFSET @p1 ROWS
                                      FETCH NEXT @p2 ROWS ONLY`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*EndpointModel
	for rows.Next() {
		var epModel EndpointModel
		err = rows.StructScan(&epModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &epModel)
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
