package connector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
)

type groupPrincipalSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

func (d *groupPrincipalSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *groupPrincipalSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	principals, nextPageToken, err := d.client.ListGroupPrincipals(ctx, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, principalModel := range principals {
		r, err := resource.NewGroupResource(
			principalModel.Name,
			d.ResourceType(ctx),
			principalModel.ID,
			nil,
			resource.WithParentResourceID(parentResourceID),
		)

		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, r)
	}

	return ret, nextPageToken, nil, nil
}

func (d *groupPrincipalSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (d *groupPrincipalSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newGroupPrincipalSyncer(ctx context.Context, c *mssqldb.Client) *groupPrincipalSyncer {
	return &groupPrincipalSyncer{
		resourceType: resourceTypeGroup,
		client:       c,
	}
}
