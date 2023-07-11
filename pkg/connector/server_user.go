package connector

import (
	"context"
	"net/mail"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	enTypes "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
)

type userPrincipalSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

var loginPermissions = map[string]string{
	"AL": "Alter",
	"CL": "Control",
	"IM": "Impersonate",
	"VW": "View Definition",
}

func (d *userPrincipalSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *userPrincipalSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	principals, nextPageToken, err := d.client.ListServerUserPrincipals(ctx, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, principalModel := range principals {
		status := v2.UserTrait_Status_STATUS_ENABLED
		if principalModel.IsDisabled {
			status = v2.UserTrait_Status_STATUS_DISABLED
		}

		userOpts := []resource.UserTraitOption{resource.WithStatus(status)}

		if _, err = mail.ParseAddress(principalModel.Name); err == nil {
			userOpts = append(userOpts, resource.WithEmail(principalModel.Name, true))
		}

		r, err := resource.NewUserResource(
			principalModel.Name,
			d.ResourceType(ctx),
			principalModel.ID,
			userOpts,
			resource.WithParentResourceID(parentResourceID),
		)
		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, r)
	}

	return ret, nextPageToken, nil, nil
}

func (d *userPrincipalSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var ret []*v2.Entitlement

	for key, name := range loginPermissions {
		ret = append(ret, enTypes.NewPermissionEntitlement(resource, key, enTypes.WithDisplayName(name)))
	}

	return ret, "", nil, nil
}

func (d *userPrincipalSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newUserPrincipalSyncer(ctx context.Context, c *mssqldb.Client) *userPrincipalSyncer {
	return &userPrincipalSyncer{
		resourceType: resourceTypeUser,
		client:       c,
	}
}
