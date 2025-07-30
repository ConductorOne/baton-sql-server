package connector

import (
	"context"
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
)

var serverPermissions = map[string]string{
	"AAES": "Alter Any Event Session",
	"ADBO": "Administer Bulk Operations",
	"ALAA": "Alter Any Server Audit",
	"ALAG": "Alter Any Availability Group",
	"ALCD": "Alter Any Credential",
	"ALCO": "Alter Any Connection",
	"ALDB": "Alter Any Database Server",
	"ALES": "Alter Any Endpoint Server",
	"ALLG": "Alter Any Login Server",
	"ALLS": "Alter Any Linked Server",
	"ALRS": "Alter Resources Server",
	"ALSR": "Alter Any Server Role",
	"ALSS": "Alter Server State",
	"ALST": "Alter Settings",
	"ALTR": "Alter Trace",
	"AUTH": "Authenticate Server",
	"CADB": "Connect Any Database",
	"CL":   "Control",
	"CO":   "Connect endpoint",
	"COSQ": "Connect SQL",
	"CRAC": "Create Availability Group",
	"CRDB": "Create Any Database",
	"CRDE": "Create DDL Event",
	"CRHE": "Create Endpoint",
	"CRSR": "Create Server Role",
	"CRTE": "Create Trace Event Notification",
	"IAL":  "Impersonate Any Login",
	"SHDN": "Shutdown",
	"SUS":  "Select All User Securables",
	"VW":   "View Any Definition",
	"VWDB": "View Any Database",
	"VWSS": "View Server State",
	"XA":   "External Access",
	"XU":   "Unsafe Assembly",
}

type serverSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

func (d *serverSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return d.resourceType
}

func (d *serverSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	server, err := d.client.GetServer(ctx)
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.Resource
	r, err := resource.NewResource(
		server.Name,
		d.ResourceType(ctx),
		server.Name,
		resource.WithAnnotation(
			// &v2.ChildResourceType{ResourceTypeId: resourceTypeEndpoint.Id},
			&v2.ChildResourceType{ResourceTypeId: resourceTypeDatabase.Id},
			&v2.ChildResourceType{ResourceTypeId: resourceTypeUser.Id},
			&v2.ChildResourceType{ResourceTypeId: resourceTypeServerRole.Id},
			&v2.ChildResourceType{ResourceTypeId: resourceTypeGroup.Id},
		),
	)
	if err != nil {
		return nil, "", nil, err
	}
	ret = append(ret, r)

	return ret, "", nil, nil
}

func (d *serverSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var ret []*v2.Entitlement

	for key, name := range serverPermissions {
		ret = append(ret, &v2.Entitlement{
			Id:          enTypes.NewEntitlementID(resource, key),
			DisplayName: name,
			Slug:        name,
			Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
			Resource:    resource,
			GrantableTo: []*v2.ResourceType{resourceTypeUser, resourceTypeGroup, resourceTypeServerRole},
		})
		ret = append(ret, &v2.Entitlement{
			Id:          enTypes.NewEntitlementID(resource, key+"-grant"),
			DisplayName: fmt.Sprintf("%s (With Grant)", name),
			Slug:        fmt.Sprintf("%s (With Grant)", name),
			Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
			Resource:    resource,
			GrantableTo: []*v2.ResourceType{resourceTypeUser, resourceTypeGroup, resourceTypeServerRole},
		})
	}

	return ret, "", nil, nil
}

func (d *serverSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var ret []*v2.Grant

	principalPerms, nextPageToken, err := d.client.ListServerPermissions(ctx, &mssqldb.Pager{Size: pToken.Size, Token: pToken.Token})
	if err != nil {
		return nil, "", nil, err
	}

	for _, p := range principalPerms {
		perms := strings.Split(p.Permissions, ",")
		for _, perm := range perms {
			perm = strings.TrimSpace(perm)
			if _, ok := serverPermissions[perm]; ok {
				rt, err := resourceTypeFromServerPrincipal(p.PrincipalType)
				if err != nil {
					return nil, "", nil, err
				}

				principal := &v2.ResourceId{
					ResourceType: rt.Id,
					Resource:     strconv.FormatInt(p.PrincipalID, 10),
				}

				grantOpts, err := BuildBatonIDGrantOptions(principal, p.PrincipalType, p.PrincipalName)
				if err != nil {
					return nil, "", nil, err
				}

				switch p.State {
				case "G":
					ret = append(ret, grTypes.NewGrant(resource, perm, principal, grantOpts...))
				case "W":
					ret = append(ret, grTypes.NewGrant(resource, perm+"-grant", principal, grantOpts...))
				}
			}
		}
	}

	return ret, nextPageToken, nil, nil
}

func newServerSyncer(ctx context.Context, c *mssqldb.Client) *serverSyncer {
	return &serverSyncer{
		resourceType: resourceTypeServer,
		client:       c,
	}
}
