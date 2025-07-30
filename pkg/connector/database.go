package connector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	enTypes "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grTypes "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type databaseSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

func (d *databaseSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *databaseSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != resourceTypeServer.Id {
		return nil, "", nil, fmt.Errorf("")
	}

	databases, nextPageToken, err := d.client.ListDatabases(ctx, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, dbModel := range databases {
		r, err := resource.NewResource(
			dbModel.Name,
			d.ResourceType(ctx),
			dbModel.ID,
			resource.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: resourceTypeDatabaseRole.Id}),
			// resource.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: resourceTypeDatabaseUser.Id}),
		)
		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, r)
	}

	return ret, nextPageToken, nil, nil
}

func (d *databaseSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var ret []*v2.Entitlement

	for key, name := range mssqldb.DatabasePermissions {
		grantSlug := fmt.Sprintf("%s (With Grant)", name)
		ret = append(ret,
			&v2.Entitlement{
				Id:          enTypes.NewEntitlementID(resource, key),
				DisplayName: name,
				Slug:        name,
				Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
				Resource:    resource,
				GrantableTo: []*v2.ResourceType{resourceTypeUser, resourceTypeGroup, resourceTypeDatabaseRole},
			},
			&v2.Entitlement{
				Id:          enTypes.NewEntitlementID(resource, key+"-grant"),
				DisplayName: grantSlug,
				Slug:        grantSlug,
				Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
				Resource:    resource,
				GrantableTo: []*v2.ResourceType{resourceTypeUser, resourceTypeGroup, resourceTypeDatabaseRole},
			})
	}

	return ret, "", nil, nil
}

func (d *databaseSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var ret []*v2.Grant

	l := ctxzap.Extract(ctx)

	dbID, err := strconv.ParseInt(resource.Id.Resource, 10, 64)
	if err != nil {
		return nil, "", nil, err
	}
	db, err := d.client.GetDatabase(ctx, dbID)
	if err != nil {
		return nil, "", nil, err
	}

	principalPerms, nextPageToken, err := d.client.ListDatabasePermissions(ctx, db.Name, &mssqldb.Pager{Size: pToken.Size, Token: pToken.Token})
	if err != nil {
		return nil, "", nil, err
	}

	for _, p := range principalPerms {
		perms := strings.Split(p.Permissions, ",")
		for _, perm := range perms {
			perm = strings.TrimSpace(perm)
			if _, ok := mssqldb.DatabasePermissions[perm]; ok {
				rt, err := resourceTypeFromDatabasePrincipal(p.PrincipalType)
				if err != nil {
					l.Error("unexpected principal type", zap.String("principal_type", p.PrincipalType))
					continue
				}

				var resourceID *v2.ResourceId
				switch rt.Id {
				case resourceTypeUser.Id, resourceTypeGroup.Id:
					serverPrincipal, err := d.client.GetServerPrincipalForDatabasePrincipal(ctx, db.Name, p.PrincipalID)
					if err != nil {
						if errors.Is(err, mssqldb.ErrNoServerPrincipal) {
							l.Debug("no server principal for database principal", zap.String("user", p.PrincipalName))
							continue
						}
						return nil, "", nil, err
					}

					resourceID = &v2.ResourceId{
						ResourceType: rt.Id,
						Resource:     serverPrincipal.ID,
					}

				case resourceTypeDatabaseRole.Id:
					resourceID = &v2.ResourceId{
						ResourceType: rt.Id,
						Resource:     fmt.Sprintf("%s:%d", db.Name, p.PrincipalID),
					}
				default:
					return nil, "", nil, fmt.Errorf("unexpected resource type: %s", rt.Id)
				}

				grantOpts, err := BuildBatonIDGrantOptions(resourceID, p.PrincipalType, p.PrincipalName)
				if err != nil {
					return nil, "", nil, err
				}

				switch p.State {
				case "G":
					ret = append(ret, grTypes.NewGrant(resource, perm, &v2.Resource{
						Id: resourceID,
					}, grantOpts...))
				case "W":
					ret = append(ret, grTypes.NewGrant(resource, perm+"-grant", &v2.Resource{
						Id: resourceID,
					}, grantOpts...))
				}
			}
		}
	}

	return ret, nextPageToken, nil, nil
}

func (d *databaseSyncer) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if resource.Id.ResourceType != resourceTypeUser.Id {
		return nil, nil, fmt.Errorf("resource type %s is not supported for granting", resource.Id.ResourceType)
	}

	splitId := strings.Split(entitlement.Id, ":")
	if len(splitId) != 3 {
		return nil, nil, fmt.Errorf("unexpected entitlement id: %s", entitlement.Id)
	}

	dbId, err := strconv.ParseInt(splitId[1], 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("unexpected database id: %s", splitId[1])
	}

	permission := splitId[2]
	isGrant := strings.Contains(permission, "-grant")
	if isGrant {
		permission = strings.Replace(permission, "-grant", "", 1)
	}

	database, err := d.client.GetDatabase(ctx, dbId)
	if err != nil {
		return nil, nil, err
	}

	user, err := d.client.GetUserPrincipal(ctx, resource.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	dbUser, err := d.client.GetUserFromDb(ctx, database.Name, resource.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	if dbUser == nil {
		l.Info("user not found in database, creating user for principal", zap.String("user", resource.Id.Resource))

		err = d.client.CreateDatabaseUserForPrincipal(ctx, database.Name, user.Name)
		if err != nil {
			return nil, nil, err
		}
	}

	err = d.client.GrantPermissionOnDatabase(ctx, permission, database.Name, user.Name)
	if err != nil {
		return nil, nil, err
	}

	entitlementName := permission
	if isGrant {
		entitlementName = permission + "-grant"
	}

	newGrant := grTypes.NewGrant(resource, entitlementName, &v2.ResourceId{
		Resource:     user.ID,
		ResourceType: resourceTypeUser.Id,
	})

	return []*v2.Grant{newGrant}, nil, nil
}

func (d *databaseSyncer) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if grant.Principal.Id.ResourceType != resourceTypeUser.Id {
		return nil, fmt.Errorf("resource type %s is not supported for revoking", grant.Principal.Id.ResourceType)
	}

	splitId := strings.Split(grant.Entitlement.Id, ":")

	dbId, err := strconv.ParseInt(splitId[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unexpected database id: %s", splitId[1])
	}

	permission := strings.Replace(splitId[2], "-grant", "", 1)

	database, err := d.client.GetDatabase(ctx, dbId)
	if err != nil {
		return nil, err
	}

	user, err := d.client.GetUserPrincipal(ctx, grant.Principal.Id.Resource)
	if err != nil {
		return nil, err
	}

	err = d.client.RevokePermissionOnDatabase(ctx, permission, database.Name, user.Name)
	if err != nil {
		return nil, err
	}

	l.Debug("revoked permission", zap.String("permission", permission), zap.String("user", user.Name), zap.String("database", database.Name))
	return nil, nil
}

func newDatabaseSyncer(ctx context.Context, c *mssqldb.Client) *databaseSyncer {
	return &databaseSyncer{
		resourceType: resourceTypeDatabase,
		client:       c,
	}
}
