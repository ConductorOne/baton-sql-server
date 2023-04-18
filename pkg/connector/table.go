package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/ConductorOne/baton-mssqldb/pkg/mssqldb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	enTypes "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

type tableSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

var tablePermissions = map[string]string{
	"AL":   "Alter",
	"CL":   "Control",
	"DL":   "Delete",
	"EX":   "Execute",
	"IN":   "Insert",
	"RC":   "Receive",
	"RF":   "References",
	"SL":   "Select",
	"TO":   "Take Ownership",
	"UP":   "Update",
	"VW":   "View Definition",
	"VWCT": "View Change Tracking",
}

func (d *tableSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *tableSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != mssqldb.SchemaType {
		return nil, "", nil, fmt.Errorf("tables must have a schema parent resource")
	}

	idParts := strings.SplitN(parentResourceID.Resource, ":", 2)
	if len(idParts) != 2 {
		return nil, "", nil, fmt.Errorf("malformed parent resource ID")
	}

	schemaID, err := strconv.ParseInt(idParts[1], 10, 64)
	if err != nil {
		return nil, "", nil, err
	}

	schema, err := d.client.GetSchema(ctx, idParts[0], schemaID)
	if err != nil {
		return nil, "", nil, err
	}

	tables, nextPageToken, err := d.client.ListTables(ctx, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size}, idParts[0], schemaID)
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, tableModel := range tables {
		r, err := resource.NewResource(
			fmt.Sprintf("%s: %s: %s", idParts[0], schema.Name, tableModel.Name),
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
	var ret []*v2.Entitlement

	for key, name := range tablePermissions {
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

func (d *tableSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newTableSyncer(ctx context.Context, c *mssqldb.Client) *tableSyncer {
	return &tableSyncer{
		resourceType: resourceTypeTable,
		client:       c,
	}
}
