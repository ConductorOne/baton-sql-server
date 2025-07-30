package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	bid "github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	enTypes "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grTypes "github.com/conductorone/baton-sdk/pkg/types/grant"
	sdkResources "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type serverRolePrincipalSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

func (d *serverRolePrincipalSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *serverRolePrincipalSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	principals, nextPageToken, err := d.client.ListServerRoles(ctx, &mssqldb.Pager{Token: pToken.Token, Size: pToken.Size})
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	for _, principalModel := range principals {
		r, err := sdkResources.NewRoleResource(
			principalModel.Name,
			d.ResourceType(ctx),
			principalModel.ID,
			nil,
			sdkResources.WithParentResourceID(parentResourceID),
		)
		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, r)
	}

	return ret, nextPageToken, nil, nil
}

func (d *serverRolePrincipalSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var ret []*v2.Entitlement

	ret = append(ret, enTypes.NewAssignmentEntitlement(
		resource,
		"member",
		enTypes.WithGrantableTo(resourceTypeUser, resourceTypeGroup, resourceTypeServerRole),
	))

	return ret, "", nil, nil
}

type roleGrantPaging struct {
	PageToken   string          `json:"page_token"`
	NestedRoles map[string]bool `json:"nested_roles"`
}

func (d *serverRolePrincipalSyncer) loadGrantPaging(token *pagination.Token) (*pagination.Bag, map[string]bool, error) {
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

func (d *serverRolePrincipalSyncer) saveGrantPaging(bag *pagination.Bag, visited map[string]bool) (string, error) {
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

func (d *serverRolePrincipalSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
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
			ResourceTypeID: resourceTypeServerRole.Id,
			ResourceID:     resource.Id.Resource,
		})

	case resourceTypeServerRole.Id:
		principals, nextPageToken, err := d.client.ListServerRolePrincipals(ctx, b.ResourceID(), &mssqldb.Pager{Token: b.PageToken(), Size: pToken.Size})
		if err != nil {
			return nil, "", nil, err
		}

		err = b.Next(nextPageToken)
		if err != nil {
			return nil, "", nil, err
		}

		for _, principal := range principals {
			var rt *v2.ResourceType

			switch principal.Type {
			case "S", "E", "C", "U":
				rt = resourceTypeUser
			case "X", "G":
				rt = resourceTypeGroup
			case "R":
				rt = resourceTypeServerRole
				pID := strconv.FormatInt(principal.ID, 10)
				if _, ok := visited[pID]; !ok {
					b.Push(pagination.PageState{
						ResourceTypeID: resourceTypeServerRole.Id,
						ResourceID:     pID,
					})
				}
			default:
				l.Error("unknown principal type", zap.String("type", principal.Type), zap.Any("principal", principal), zap.String("role_id", b.ResourceID()))
				continue
			}

			principalID, err := sdkResources.NewResourceID(rt, principal.ID)
			if err != nil {
				return nil, "", nil, err
			}

			grantOpts, err := BuildBatonIDGrantOptions(principalID, principal.Type, principal.Name)
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

func BuildBatonIDGrantOptions(principalID *v2.ResourceId, principalType string, principalName string) ([]grTypes.GrantOption, error) {
	grantOpts := []grTypes.GrantOption{}

	switch principalType {
	case "G": // Configure BatonID matching for Active Directory groups
		gr := &v2.Resource{
			Id: principalID,
		}

		ent := enTypes.NewAssignmentEntitlement(gr, "member")
		bidEnt, err := bid.MakeBid(ent)
		if err != nil {
			return nil, err
		}

		grantOpts = append(grantOpts,
			grTypes.WithAnnotation(&v2.ExternalResourceMatch{
				ResourceType: v2.ResourceType_TRAIT_GROUP,
				Key:          "downlevel_logon_name",
				Value:        principalName,
			}),
			grTypes.WithAnnotation(&v2.GrantExpandable{
				EntitlementIds: []string{bidEnt},
				Shallow:        true,
			}),
		)
	case "U": // Configure BatonID matching for Active Directory users
		grantOpts = append(grantOpts,
			grTypes.WithAnnotation(&v2.ExternalResourceMatch{
				ResourceType: v2.ResourceType_TRAIT_USER,
				Key:          "downlevel_logon_name",
				Value:        principalName,
			}),
		)
	}

	return grantOpts, nil
}

func (d *serverRolePrincipalSyncer) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	var err error

	if resource.Id.ResourceType != resourceTypeUser.Id {
		return nil, nil, fmt.Errorf("resource type %s is not supported for granting", resource.Id.ResourceType)
	}

	// database-role:baton_test:6:member
	splitId := strings.Split(entitlement.Id, ":")
	if len(splitId) < 2 {
		return nil, nil, fmt.Errorf("unexpected entitlement id: %s", entitlement.Id)
	}

	roleId := splitId[len(splitId)-2]

	var role *mssqldb.RoleModel

	role, err = d.client.GetServerRole(ctx, roleId)
	if err != nil {
		return nil, nil, err
	}

	err = d.client.AddUserToServerRole(ctx, role.Name, resource.Id.Resource)
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

func (d *serverRolePrincipalSyncer) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	userId := grant.Principal.Id.Resource

	user, err := d.client.GetUserPrincipal(ctx, userId)
	if err != nil {
		return nil, err
	}

	// database-role:baton_test:6:member
	splitId := strings.Split(grant.Entitlement.Id, ":")
	if len(splitId) < 2 {
		return nil, fmt.Errorf("unexpected entitlement id: %s", grant.Entitlement.Id)
	}

	roleId := splitId[len(splitId)-2]

	role, err := d.client.GetServerRole(ctx, roleId)
	if err != nil {
		return nil, err
	}

	err = d.client.RevokeUserToServerRole(ctx, role.Name, user.Name)
	if err != nil {
		return nil, err
	}

	return nil, err
}

func newServerRolePrincipalSyncer(ctx context.Context, c *mssqldb.Client) *serverRolePrincipalSyncer {
	return &serverRolePrincipalSyncer{
		resourceType: resourceTypeServerRole,
		client:       c,
	}
}
