package connector

import (
	"context"
	"fmt"

	"github.com/ConductorOne/baton-mssqldb/pkg/mssqldb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	enTypes "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

var endpointPermissions = map[string]string{
	"AL": "Alter",
	"CL": "Control",
	"CO": "Connect",
	"TO": "Take Ownership",
	"VW": "View Definition",
}

type endpointSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

func (d *endpointSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *endpointSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != mssqldb.ServerType {
		return nil, "", nil, fmt.Errorf("endpoints must have a database parent resource")
	}

	endpoints, nextPageToken, err := d.client.ListEndpoints(ctx, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, endpointModel := range endpoints {
		r, err := resource.NewResource(
			endpointModel.Name,
			d.ResourceType(ctx),
			endpointModel.ID,
			resource.WithParentResourceID(parentResourceID),
		)
		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, r)
	}

	return ret, nextPageToken, nil, nil
}

func (d *endpointSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var ret []*v2.Entitlement

	for key, name := range endpointPermissions {
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

func (d *endpointSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newEndpointSyncer(ctx context.Context, c *mssqldb.Client) *endpointSyncer {
	return &endpointSyncer{
		resourceType: resourceTypeEndpoint,
		client:       c,
	}
}
