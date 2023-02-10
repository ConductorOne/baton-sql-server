package mssqldb

import (
	"context"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

const ServerType = "server"

type ServerModel struct {
	Name string `db:"ServerName"`
}

func (c *Client) GetServer(ctx context.Context) (*ServerModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing server info")

	var sb strings.Builder
	sb.WriteString(`SELECT SERVERPROPERTY('ServerName') AS [ServerName]`)

	row := c.db.QueryRowxContext(ctx, sb.String())
	if row.Err() != nil {
		return nil, row.Err()
	}

	var ret ServerModel
	err := row.StructScan(&ret)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}
