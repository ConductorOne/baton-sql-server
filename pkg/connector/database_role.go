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

type databaseRolePrincipalSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

func (d *databaseRolePrincipalSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *databaseRolePrincipalSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != resourceTypeDatabase.Id {
		return nil, "", nil, fmt.Errorf("database roles must have a database as the parent resource")
	}

	dbID, err := strconv.ParseInt(parentResourceID.Resource, 10, 64)
	if err != nil {
		return nil, "", nil, err
	}
	db, err := d.client.GetDatabase(ctx, dbID)
	if err != nil {
		return nil, "", nil, err
	}

	principals, nextPageToken, err := d.client.ListDatabaseRolePrincipals(ctx, db.Name, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, principalModel := range principals {
		r, err := resource.NewRoleResource(
			fmt.Sprintf("%s (%s)", principalModel.Name, db.Name),
			d.ResourceType(ctx),
			fmt.Sprintf("%s:%d", db.Name, principalModel.ID),
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

func (d *databaseRolePrincipalSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var ret []*v2.Entitlement

	ret = append(ret, enTypes.NewAssignmentEntitlement(resource, "member"))

	return ret, "", nil, nil
}

func (d *databaseRolePrincipalSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newDatabaseRolePrincipalSyncer(ctx context.Context, c *mssqldb.Client) *databaseRolePrincipalSyncer {
	return &databaseRolePrincipalSyncer{
		resourceType: resourceTypeDatabaseRole,
		client:       c,
	}
}
