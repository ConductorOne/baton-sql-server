package connector

import (
	"github.com/ConductorOne/baton-mssqldb/pkg/mssqldb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

var (
	resourceTypeServer = &v2.ResourceType{
		Id:          mssqldb.ServerType,
		DisplayName: "Server",
	}
	resourceTypeEndpoint = &v2.ResourceType{
		Id:          mssqldb.EndpointType,
		DisplayName: "Endpoint",
	}
	resourceTypeDatabase = &v2.ResourceType{
		Id:          mssqldb.DatabaseType,
		DisplayName: "Database",
	}

	resourceTypeSchema = &v2.ResourceType{
		Id:          mssqldb.SchemaType,
		DisplayName: "Schema",
	}
	resourceTypeTable = &v2.ResourceType{
		Id:          mssqldb.TableType,
		DisplayName: "Table",
	}
	resourceTypeUser = &v2.ResourceType{
		Id:          mssqldb.UserType,
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
	}
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
