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

type databaseSyncer struct {
	resourceType *v2.ResourceType
	client       *mssqldb.Client
}

var databasePermissions = map[string]string{
	"AADS": "Alter Any Database Event Session",
	"AAMK": "Alter Any Mask",
	"AEDS": "Alter Any External Data Source",
	"AEFF": "Alter Any External File Format",
	"AL":   "Alter",
	"ALAK": "Alter Any Asymmetric Key",
	"ALAR": "Alter Any Application Role",
	"ALAS": "Alter Any Assembly",
	"ALCF": "Alter Any Certificate",
	"ALDS": "Alter Any Dataspace",
	"ALED": "Alter Any Database Event Notification",
	"ALFT": "Alter Any Fulltext Catalog",
	"ALMT": "Alter Any Message Type",
	"ALRL": "Alter Any Role",
	"ALRT": "Alter Any Route",
	"ALSB": "Alter Any Remote Service Binding",
	"ALSC": "Alter Any Contract",
	"ALSK": "Alter Any Symmetric Key",
	"ALSM": "Alter Any Schema",
	"ALSV": "Alter Any Service",
	"ALTG": "Alter Any Database DDL Trigger",
	"ALUS": "Alter Any User",
	"AUTH": "Authenticate",
	"BADB": "Backup Database",
	"BALO": "Backup Log",
	"CL":   "Control",
	"CO":   "Connect",
	"CORP": "Connect Replication",
	"CP":   "Checkpoint",
	"CRAG": "Create Aggregate",
	"CRAK": "Create Asymmetric Key",
	"CRAS": "Create Certificate",
	"CRDB": "Create Fatabase",
	"CRDF": "Create Default",
	"CRED": "Create Database DDL Event Notification",
	"CRFN": "Create Function",
	"CRFT": "Create Fulltext Catalog",
	"CRMT": "Create Message Type",
	"CRPR": "Create Procedure",
	"CRQU": "Create Queue",
	"CRRL": "Create Role",
	"CRRT": "Create Route",
	"CRRU": "Create Rule",
	"CRSB": "Create Remote Service Binding",
	"CRSC": "Create contract",
	"CRSK": "Create symmetric key",
	"CRSM": "Create Schema",
	"CRSN": "Create Synonym",
	"CRSO": "Create Sequence",
	"CRSV": "Create Service",
	"CRTB": "Create Table",
	"CRTY": "Create Type",
	"CRVW": "Create View",
	"CRXS": "Create XML Schema Collection",
	"DABO": "Administer Database Bulk Operations",
	"EAES": "Execute Any External Script",
	"EX":   "Execute",
	"IN":   "Insert",
	"RC":   "Receive Object",
	"RF":   "References",
	"SL":   "Select",
	"SPLN": "Showplan",
	"SUQN": "Subscribe Query Notifications",
	"TO":   "Take Ownership",
	"UP":   "Update",
	"VW":   "View Definition",
	"VWCK": "View Any Column Encryption Key Definition",
	"VWCM": "View Any Column Master Key Definition",
	"VWCT": "View Change Tracking",
	"VWDS": "View Database State Database",
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
			resource.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: resourceTypeTable.Id}),
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

	for key, name := range databasePermissions {
		ret = append(ret, enTypes.NewPermissionEntitlement(resource, key, enTypes.WithDisplayName(name)))
	}

	return ret, "", nil, nil
}

func (d *databaseSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newDatabaseSyncer(ctx context.Context, c *mssqldb.Client) *databaseSyncer {
	return &databaseSyncer{
		resourceType: resourceTypeDatabase,
		client:       c,
	}
}
