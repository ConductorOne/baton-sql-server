package connector

import (
	"context"

	"github.com/ConductorOne/baton-mssqldb/pkg/mssqldb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
)

type Mssqldb struct {
	client *mssqldb.Client
}

// Resource model:
// Server
// |-- Principals (User, Group, Role)
//    |-- Permissions
// |-- Databases
//    |-- Principals
//    |-- Users

func (o *Mssqldb) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	var annos annotations.Annotations

	return &v2.ConnectorMetadata{
		DisplayName: "Microsoft SQL Server",
		Annotations: annos,
	}, nil
}

func (o *Mssqldb) Validate(ctx context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func (o *Mssqldb) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		newServerSyncer(ctx, o.client),
		newDatabaseSyncer(ctx, o.client),
		newUserPrincipalSyncer(ctx, o.client),
		newServerRolePrincipalSyncer(ctx, o.client),
		newDatabaseRolePrincipalSyncer(ctx, o.client),
		newGroupPrincipalSyncer(ctx, o.client),
	}
}

func New(ctx context.Context, dsn string) (*Mssqldb, error) {
	c, err := mssqldb.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Mssqldb{
		client: c,
	}, nil
}
