package connector

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ConductorOne/baton-mssqldb/pkg/mssqldb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

type tableSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

func (d *tableSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *tableSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != mssqldb.DatabaseType {
		return nil, "", nil, fmt.Errorf("tables must have a database parent resource")
	}

	dbID, err := strconv.ParseInt(parentResourceID.Resource, 10, 64)
	if err != nil {
		return nil, "", nil, err
	}
	db, err := d.client.GetDatabase(ctx, dbID)
	if err != nil {
		return nil, "", nil, err
	}

	tables, nextPageToken, err := d.client.ListTables(ctx, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size}, db.Name)
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, tableModel := range tables {
		r, err := resource.NewResource(
			tableModel.Name,
			d.ResourceType(ctx),
			tableModel.ID,
			resource.WithParentResourceID(parentResourceID),
		)
		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, r)
	}

	return ret, nextPageToken, nil, nil
}

func (d *tableSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (d *tableSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newTableSyncer(ctx context.Context, c *mssqldb.Client) *tableSyncer {
	return &tableSyncer{
		resourceType: resourceTypeTable,
		client:       c,
	}
}
