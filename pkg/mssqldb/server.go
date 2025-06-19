package mssqldb

import (
	"context"
	"fmt"
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
	_, _ = sb.WriteString(`SELECT SERVERPROPERTY('ServerName') AS [ServerName]`)

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

func (c *Client) DisableUserFromServer(ctx context.Context, userName string) error {
	if strings.ContainsAny(userName, "[]\"';") {
		return fmt.Errorf("invalid characters in userName")
	}

	query := fmt.Sprintf(`
ALTER LOGIN [%s] DISABLE;`, userName)

	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}
