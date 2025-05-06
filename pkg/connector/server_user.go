package connector

import (
	"context"
	"fmt"
	"net/mail"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	_ "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	enTypes "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sql-server/pkg/mssqldb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// userPrincipalSyncer implements both ResourceSyncer and AccountManager.
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

// CreateAccount creates a SQL Server login and database user for an Active Directory user.
// It implements the AccountManager interface.
func (d *userPrincipalSyncer) CreateAccount(
	ctx context.Context,
	accountInfo *v2.AccountInfo,
	credentialOptions *v2.CredentialOptions,
) (connectorbuilder.CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	// Extract required username field from profile
	usernameVal := accountInfo.Profile.GetFields()["username"]
	if usernameVal == nil || usernameVal.GetStringValue() == "" {
		return nil, nil, nil, fmt.Errorf("missing required username field")
	}
	username := usernameVal.GetStringValue()

	// Extract optional domain field from profile
	var domain string
	domainVal := accountInfo.Profile.GetFields()["domain"]
	if domainVal != nil && domainVal.GetStringValue() != "" {
		domain = domainVal.GetStringValue()
	}

	// Create the Windows login
	err := d.client.CreateWindowsLogin(ctx, domain, username)
	if err != nil {
		l.Error("Failed to create Windows login", zap.Error(err))
		return nil, nil, nil, fmt.Errorf("failed to create Windows login: %w", err)
	}

	// Determine the formatted username for the database user
	var formattedUsername string
	if domain != "" {
		formattedUsername = fmt.Sprintf("%s\\%s", domain, username)
	} else {
		formattedUsername = username
	}

	// Get list of databases to create users in
	databases, _, err := d.client.ListDatabases(ctx, &mssqldb.Pager{})
	if err != nil {
		l.Error("Failed to retrieve databases", zap.Error(err))
		errMsg := fmt.Sprintf("Login created successfully, but failed to retrieve databases: %v", err)
		result := &v2.CreateAccountResponse_ActionRequiredResult{
			Message:               errMsg,
			IsCreateAccountResult: true,
		}
		return result, nil, nil, nil
	}

	// Create user in each database
	var dbsCreated []string
	for _, db := range databases {
		// Skip system databases
		if db.Name == "master" || db.Name == "tempdb" || db.Name == "model" || db.Name == "msdb" {
			continue
		}

		err = d.client.CreateDatabaseUserForPrincipal(ctx, db.Name, formattedUsername)
		if err != nil {
			l.Error("Failed to create user in database",
				zap.String("database", db.Name),
				zap.String("user", formattedUsername),
				zap.Error(err))
			errMsg := fmt.Sprintf("Login created successfully, but failed to create user in some databases: %v", err)
			result := &v2.CreateAccountResponse_ActionRequiredResult{
				Message:               errMsg,
				IsCreateAccountResult: true,
			}
			return result, nil, nil, nil
		}
		dbsCreated = append(dbsCreated, db.Name)
	}

	// Create a resource for the newly created login
	profile := map[string]interface{}{
		"username":        username,
		"domain":          domain,
		"formatted_login": formattedUsername,
		"databases":       dbsCreated,
	}

	// Use email as name if it looks like an email address
	var userOpts []resource.UserTraitOption
	userOpts = append(userOpts, resource.WithUserProfile(profile))
	userOpts = append(userOpts, resource.WithStatus(v2.UserTrait_Status_STATUS_ENABLED))

	if _, err = mail.ParseAddress(username); err == nil {
		userOpts = append(userOpts, resource.WithEmail(username, true))
	}

	// Create a resource object to represent the user
	resource, err := resource.NewUserResource(
		formattedUsername,
		d.ResourceType(ctx),
		formattedUsername, // Use the formatted username as the ID
		userOpts,
	)
	if err != nil {
		l.Error("Failed to create resource for new user", zap.Error(err))
		return nil, nil, nil, fmt.Errorf("failed to create resource for new user: %w", err)
	}

	// Return success result with the new user resource
	successResult := &v2.CreateAccountResponse_SuccessResult{
		Resource:              resource,
		IsCreateAccountResult: true,
	}

	return successResult, nil, nil, nil
}

// CreateAccountCapabilityDetails returns the capability details for account creation.
func (d *userPrincipalSyncer) CreateAccountCapabilityDetails(
	ctx context.Context,
) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return &v2.CredentialDetailsAccountProvisioning{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
	}, nil, nil
}

func newUserPrincipalSyncer(ctx context.Context, c *mssqldb.Client) *userPrincipalSyncer {
	return &userPrincipalSyncer{
		resourceType: resourceTypeUser,
		client:       c,
	}
}
