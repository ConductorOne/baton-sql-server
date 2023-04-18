package connector

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func resourceTypeFromServerPrincipal(pType string) (*v2.ResourceType, error) {
	switch pType {
	case "R":
		return resourceTypeServerRole, nil
	case "G", "X":
		return resourceTypeGroup, nil
	case "S", "U", "C", "E", "K":
		return resourceTypeUser, nil
	default:
		return nil, fmt.Errorf("unknown principal type: %s", pType)
	}
}

func resourceTypeFromDatabasePrincipal(pType string) (*v2.ResourceType, error) {
	switch pType {
	case "R":
		return resourceTypeDatabaseRole, nil
	case "G", "X":
		return resourceTypeGroup, nil
	case "S", "U", "C", "E", "K":
		return resourceTypeDatabaseUser, nil
	default:
		return nil, fmt.Errorf("unknown principal type: %s", pType)
	}
}
