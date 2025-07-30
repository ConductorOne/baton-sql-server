package connector

import (
	"context"
	"encoding/json"
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
	sdkResources "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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

	principals, nextPageToken, err := d.client.ListDatabaseRoles(ctx, db.Name, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size})
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

	grantableTo := enTypes.WithGrantableTo(resourceTypeUser, resourceTypeGroup, resourceTypeDatabaseRole)
	ret = append(ret, enTypes.NewAssignmentEntitlement(resource, "member", grantableTo))

	return ret, "", nil, nil
}

func (d *databaseRolePrincipalSyncer) loadGrantPaging(token *pagination.Token) (*pagination.Bag, map[string]bool, error) {
	gPaging := roleGrantPaging{}

	if token != nil && token.Token != "" {
		err := json.Unmarshal([]byte(token.Token), &gPaging)
		if err != nil {
			return nil, nil, err
		}
	} else {
		gPaging.NestedRoles = make(map[string]bool)
	}

	b := &pagination.Bag{}
	if err := b.Unmarshal(gPaging.PageToken); err != nil {
		return nil, nil, err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: "init",
		})
	}

	return b, gPaging.NestedRoles, nil
}

func (d *databaseRolePrincipalSyncer) saveGrantPaging(bag *pagination.Bag, visited map[string]bool) (string, error) {
	bagToken, err := bag.Marshal()
	if err != nil {
		return "", err
	}

	if bagToken == "" {
		return "", nil
	}

	gPaging := roleGrantPaging{
		PageToken:   bagToken,
		NestedRoles: visited,
	}

	nextToken, err := json.Marshal(gPaging)
	if err != nil {
		return "", err
	}

	return string(nextToken), nil
}

func (d *databaseRolePrincipalSyncer) Grants(
	ctx context.Context,
	resource *v2.Resource,
	pToken *pagination.Token,
) ([]*v2.Grant, string, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	var ret = []*v2.Grant{}

	b, visited, err := d.loadGrantPaging(pToken)
	if err != nil {
		return nil, "", nil, err
	}

	switch b.ResourceTypeID() {
	case "init":
		b.Pop()
		b.Push(pagination.PageState{
			ResourceTypeID: resourceTypeDatabaseRole.Id,
			ResourceID:     resource.Id.Resource,
		})

	case resourceTypeDatabaseRole.Id:
		idParts := strings.Split(b.ResourceID(), ":")
		if len(idParts) != 2 {
			return nil, "", nil, fmt.Errorf("invalid database role id: %s", b.ResourceID())
		}
		principals, nextPageToken, err := d.client.ListDatabaseRolePrincipals(
			ctx,
			idParts[0],
			idParts[1],
			&mssqldb.Pager{Token: b.PageToken(), Size: pToken.Size},
		)
		if err != nil {
			return nil, "", nil, err
		}

		err = b.Next(nextPageToken)
		if err != nil {
			return nil, "", nil, err
		}

		for _, dbPrincipal := range principals {
			var principalID *v2.ResourceId

			switch dbPrincipal.Type {
			case "S", "E", "K", "C", "U", "X", "G":
				serverPrincipal, err := d.client.GetServerPrincipalForDatabasePrincipal(ctx, idParts[0], dbPrincipal.ID)
				if err != nil {
					if errors.Is(err, mssqldb.ErrNoServerPrincipal) {
						l.Debug("no server principal for database principal", zap.String("user", dbPrincipal.Name), zap.String("role_id", b.ResourceID()))
						continue
					}
					return nil, "", nil, err
				}

				rt := resourceTypeUser

				if dbPrincipal.Type == "G" || dbPrincipal.Type == "X" {
					rt = resourceTypeGroup
				}

				principalID, err = sdkResources.NewResourceID(rt, serverPrincipal.ID)
				if err != nil {
					return nil, "", nil, err
				}

			case "R":
				pID := strconv.FormatInt(dbPrincipal.ID, 10)
				if _, ok := visited[pID]; !ok {
					b.Push(pagination.PageState{
						ResourceTypeID: resourceTypeDatabaseRole.Id,
						ResourceID:     fmt.Sprintf("%s:%s", idParts[0], pID),
					})
				}
				principalID, err = sdkResources.NewResourceID(resourceTypeDatabaseRole, fmt.Sprintf("%s:%d", idParts[0], dbPrincipal.ID))
				if err != nil {
					return nil, "", nil, err
				}
			default:
				l.Error("unknown db principal type", zap.String("type", dbPrincipal.Type), zap.Any("db_principal", dbPrincipal), zap.String("role_id", b.ResourceID()))
				continue
			}

			if principalID == nil {
				return nil, "", nil, fmt.Errorf("invalid state: principalID is nil")
			}

			grantOpts, err := BuildBatonIDGrantOptions(principalID, dbPrincipal.Type, dbPrincipal.Name)
			if err != nil {
				return nil, "", nil, err
			}

			ret = append(ret, grTypes.NewGrant(resource, "member", principalID, grantOpts...))
		}

		visited[b.ResourceID()] = true

	default:
		return nil, "", nil, fmt.Errorf("unexpected pagination state")
	}

	npt, err := d.saveGrantPaging(b, visited)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, npt, nil, nil
}

func (d *databaseRolePrincipalSyncer) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	var err error

	l := ctxzap.Extract(ctx)

	if resource.Id.ResourceType != resourceTypeUser.Id {
		return nil, nil, fmt.Errorf("resource type %s is not supported for granting", resource.Id.ResourceType)
	}

	// database-role:baton_test:6:member
	splitId := strings.Split(entitlement.Id, ":")
	if len(splitId) != 4 {
		return nil, nil, fmt.Errorf("unexpected entitlement id: %s", entitlement.Id)
	}

	dbName := splitId[1]
	roleId := splitId[2]

	var role *mssqldb.RoleModel

	role, err = d.client.GetDatabaseRole(ctx, dbName, roleId)
	if err != nil {
		return nil, nil, err
	}

	dbUser, err := d.client.GetUserFromDb(ctx, dbName, resource.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	if dbUser == nil {
		l.Info("user not found in database, creating user for principal", zap.String("user", resource.Id.Resource))

		user, err := d.client.GetUserPrincipal(ctx, resource.Id.Resource)
		if err != nil {
			return nil, nil, err
		}

		err = d.client.CreateDatabaseUserForPrincipal(ctx, dbName, user.Name)
		if err != nil {
			return nil, nil, err
		}
	}

	err = d.client.AddUserToDatabaseRole(ctx, role.Name, dbName, resource.Id.Resource)
	if err != nil {
		return nil, nil, err
	}

	grants := []*v2.Grant{
		grTypes.NewGrant(resource, "member", &v2.ResourceId{
			Resource:     resource.Id.Resource,
			ResourceType: resourceTypeUser.Id,
		}),
	}

	return grants, nil, nil
}

func (d *databaseRolePrincipalSyncer) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	userId := grant.Principal.Id.Resource

	user, err := d.client.GetUserPrincipal(ctx, userId)
	if err != nil {
		return nil, err
	}

	// database-role:baton_test:6:member
	splitId := strings.Split(grant.Entitlement.Id, ":")
	if len(splitId) != 4 {
		return nil, fmt.Errorf("unexpected entitlement id: %s", grant.Entitlement.Id)
	}

	dbName := splitId[1]
	roleId := splitId[2]

	role, err := d.client.GetDatabaseRole(ctx, dbName, roleId)
	if err != nil {
		return nil, err
	}

	err = d.client.RevokeUserToDatabaseRole(ctx, role.Name, dbName, user.Name)
	if err != nil {
		return nil, err
	}

	return nil, err
}

func newDatabaseRolePrincipalSyncer(ctx context.Context, c *mssqldb.Client) *databaseRolePrincipalSyncer {
	return &databaseRolePrincipalSyncer{
		resourceType: resourceTypeDatabaseRole,
		client:       c,
	}
}
