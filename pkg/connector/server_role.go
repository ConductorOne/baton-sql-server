package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/ConductorOne/baton-mssqldb/pkg/mssqldb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	enTypes "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grTypes "github.com/conductorone/baton-sdk/pkg/types/grant"
	sdkResources "github.com/conductorone/baton-sdk/pkg/types/resource"
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

	ret = append(ret, enTypes.NewAssignmentEntitlement(resource, "member"))

	return ret, "", nil, nil
}

type grantPaging struct {
	PageToken   string          `json:"page_token"`
	NestedRoles map[string]bool `json:"nested_roles"`
}

func (d *serverRolePrincipalSyncer) loadGrantPaging(token *pagination.Token) (*pagination.Bag, map[string]bool, error) {
	gPaging := grantPaging{}

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

	gPaging := grantPaging{
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

			ret = append(ret, grTypes.NewGrant(resource, "member", principalID))
		}

		visited[resource.Id.Resource] = true

	default:
		return nil, "", nil, fmt.Errorf("unexpected pagination state")
	}

	npt, err := d.saveGrantPaging(b, visited)
	if err != nil {
		return nil, "", nil, err
	}

	return ret, npt, nil, nil
}

func newServerRolePrincipalSyncer(ctx context.Context, c *mssqldb.Client) *serverRolePrincipalSyncer {
	return &serverRolePrincipalSyncer{
		resourceType: resourceTypeServerRole,
		client:       c,
	}
}
