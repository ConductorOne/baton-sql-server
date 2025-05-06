package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
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

	serverInfo, err := o.client.GetServer(ctx)
	if err != nil {
		return nil, err
	}

	return &v2.ConnectorMetadata{
		DisplayName: fmt.Sprintf("Microsoft SQL Server (%s)", serverInfo.Name),
		Annotations: annos,
		Description: "Baton connector for Microsoft SQL Server connector",
		AccountCreationSchema: &v2.ConnectorAccountCreationSchema{
			FieldMap: map[string]*v2.ConnectorAccountCreationSchema_Field{
				"login_type": {
					DisplayName: "Login Type",
					Required:    true,
					Description: "The type of SQL Server authentication to use (WINDOWS, SQL, AZURE_AD, or ENTRA_ID).",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "WINDOWS",
					Order:       1,
				},
				"domain": {
					DisplayName: "Active Directory Domain",
					Required:    false,
					Description: "The Active Directory domain for the user. Only used for Windows Authentication.",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "DOMAIN",
					Order:       2,
				},
				"username": {
					DisplayName: "Username",
					Required:    true,
					Description: "The username for which to create a SQL Server login.",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "username",
					Order:       3,
				},
			},
		},
	}, nil
}

func (o *Mssqldb) Validate(ctx context.Context) (annotations.Annotations, error) {
	_, err := o.client.GetServer(ctx)
	if err != nil {
		return nil, err
	}

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

func New(ctx context.Context, dsn string, skipUnavailableDatabases bool) (*Mssqldb, error) {
	c, err := mssqldb.New(ctx, dsn, skipUnavailableDatabases)
	if err != nil {
		return nil, err
	}
	return &Mssqldb{
		client: c,
	}, nil
}
