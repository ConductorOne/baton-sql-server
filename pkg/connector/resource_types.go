package connector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
)

var (
	resourceTypeServer = &v2.ResourceType{
		Id:          mssqldb.ServerType,
		DisplayName: "Server",
	}
	resourceTypeDatabase = &v2.ResourceType{
		Id:          mssqldb.DatabaseType,
		DisplayName: "Database",
	}
	resourceTypeUser = &v2.ResourceType{
		Id:          mssqldb.UserType,
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
	}
	// Groups have a user trait because represent external group identities.
	resourceTypeGroup = &v2.ResourceType{
		Id:          mssqldb.GroupType,
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}
	resourceTypeServerRole = &v2.ResourceType{
		Id:          mssqldb.ServerRoleType,
		DisplayName: "Server Role",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE},
	}
	resourceTypeDatabaseRole = &v2.ResourceType{
		Id:          mssqldb.DatabaseRoleType,
		DisplayName: "Database Role",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE},
	}
)
