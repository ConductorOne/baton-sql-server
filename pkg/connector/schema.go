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
	enTypes "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

type schemaSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

var schemaPermissions = map[string]string{
	"AL":   "Alter",
	"CL":   "Control",
	"DL":   "Delete",
	"EX":   "Execute",
	"IN":   "Insert",
	"RF":   "References",
	"SL":   "Select",
	"TO":   "Take Ownership",
	"UP":   "Update",
	"VW":   "View Definition",
	"VWCT": "View Change Tracking",
}

func (d *schemaSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *schemaSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != mssqldb.DatabaseType {
		return nil, "", nil, fmt.Errorf("schemas must have a database parent resource")
	}

	dbID, err := strconv.ParseInt(parentResourceID.Resource, 10, 64)
	if err != nil {
		return nil, "", nil, err
	}
	db, err := d.client.GetDatabase(ctx, dbID)
	if err != nil {
		return nil, "", nil, err
	}

	schemas, nextPageToken, err := d.client.ListSchemas(ctx, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size}, db.Name)
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, schemaModel := range schemas {
		r, err := resource.NewResource(
			fmt.Sprintf("%s (%s)", schemaModel.Name, db.Name),
			d.ResourceType(ctx),
			fmt.Sprintf("%s:%d", db.Name, schemaModel.ID),
			resource.WithParentResourceID(parentResourceID),
			resource.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: resourceTypeTable.Id}),
		)
		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, r)
	}

	return ret, nextPageToken, nil, nil
}

func (d *schemaSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var ret []*v2.Entitlement

	for key, name := range schemaPermissions {
		ret = append(ret, &v2.Entitlement{
			Id:          enTypes.NewEntitlementID(resource, key),
			DisplayName: name,
			Slug:        name,
			Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
			Resource:    resource,
		})
	}

	return ret, "", nil, nil
}

func (d *schemaSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newSchemaSyncer(ctx context.Context, c *mssqldb.Client) *schemaSyncer {
	return &schemaSyncer{
		resourceType: resourceTypeSchema,
		client:       c,
	}
}
