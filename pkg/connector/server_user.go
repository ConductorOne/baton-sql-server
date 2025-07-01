package connector

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
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

var _ connectorbuilder.ResourceDeleter = (*userPrincipalSyncer)(nil)

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

// CreateAccount creates a SQL Server login based on the specified login type.
// It implements the AccountManager interface.
func (d *userPrincipalSyncer) CreateAccount(
	ctx context.Context,
	accountInfo *v2.AccountInfo,
	credentialOptions *v2.CredentialOptions,
) (connectorbuilder.CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	// Extract required login_type field from profile
	loginTypeVal := accountInfo.Profile.GetFields()["login_type"]
	if loginTypeVal == nil || loginTypeVal.GetStringValue() == "" {
		return nil, nil, nil, fmt.Errorf("missing required login_type field")
	}
	loginTypeStr := loginTypeVal.GetStringValue()
	loginType := mssqldb.LoginType(loginTypeStr)

	// Extract required username field from profile
	usernameVal := accountInfo.Profile.GetFields()["username"]
	if usernameVal == nil || usernameVal.GetStringValue() == "" {
		return nil, nil, nil, fmt.Errorf("missing required username field")
	}
	username := usernameVal.GetStringValue()

	// Extract optional domain field (for Windows auth) or password (for SQL auth)
	var domain, password string
	var formattedUsername string

	switch loginType {
	case mssqldb.LoginTypeWindows:
		// For Windows auth, extract domain
		domainVal := accountInfo.Profile.GetFields()["domain"]
		if domainVal != nil && domainVal.GetStringValue() != "" {
			domain = domainVal.GetStringValue()
		}

		if domain != "" {
			formattedUsername = fmt.Sprintf("%s\\%s", domain, username)
		} else {
			formattedUsername = username
		}
	case mssqldb.LoginTypeSQL:
		// For SQL auth, generate a strong random password
		password = generateStrongPassword()
		l.Debug("generated random password for SQL Server authentication")
		formattedUsername = username
	case mssqldb.LoginTypeAzureAD, mssqldb.LoginTypeEntraID:
		// For Azure AD or Entra ID, just use the username as is
		formattedUsername = username
	default:
		return nil, nil, nil, fmt.Errorf("unsupported login type: %s", loginType)
	}

	// Create the login
	err := d.client.CreateLogin(ctx, loginType, domain, username, password)
	if err != nil {
		l.Error("Failed to create login", zap.Error(err), zap.String("loginType", string(loginType)))
		return nil, nil, nil, fmt.Errorf("failed to create login: %w", err)
	}

	uid, err := d.client.GetUserPrincipalByName(ctx, username)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get user: w", err)
	}

	// Create a resource for the newly created login
	profile := map[string]interface{}{
		"username":        username,
		"login_type":      string(loginType),
		"formatted_login": formattedUsername,
	}

	// Add domain if it exists (for Windows auth)
	if domain != "" {
		profile["domain"] = domain
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
		uid.ID,
		userOpts,
	)
	if err != nil {
		l.Error("Failed to create resource for new user", zap.Error(err))
		return nil, nil, nil, fmt.Errorf("failed to create resource for new user: %w", err)
	}

	// Prepare the response - for SQL auth, we need to return the generated password
	successResult := &v2.CreateAccountResponse_SuccessResult{
		Resource:              resource,
		IsCreateAccountResult: true,
	}

	var plaintextData []*v2.PlaintextData
	// If this is SQL authentication, return the generated password
	if loginType == mssqldb.LoginTypeSQL {
		plaintextData = []*v2.PlaintextData{
			{
				Name:        "password",
				Description: "The generated password for SQL Server authentication",
				Schema:      "text/plain",
				Bytes:       []byte(password),
			},
		}
	}

	return successResult, plaintextData, nil, nil
}

// CreateAccountCapabilityDetails returns the capability details for account creation.
func (d *userPrincipalSyncer) CreateAccountCapabilityDetails(
	ctx context.Context,
) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return &v2.CredentialDetailsAccountProvisioning{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,     // For Windows/Azure AD/Entra ID
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD, // For SQL Server auth
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
	}, nil, nil
}

func (d *userPrincipalSyncer) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	user, err := d.client.GetUserPrincipal(ctx, resourceId.GetResource())
	if err != nil {
		return nil, err
	}

	err = d.client.DisableUserFromServer(ctx, user.Name)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// generateStrongPassword creates a secure random password for SQL Server.
// The password meets SQL Server complexity requirements:
// - At least 8 characters in length
// - Contains uppercase, lowercase, numbers, and special characters.
func generateStrongPassword() string {
	const (
		uppercaseChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		lowercaseChars = "abcdefghijklmnopqrstuvwxyz"
		numberChars    = "0123456789"
		specialChars   = "!@#$%^&*()-_=+[]{}|;:,.<>?"
		passwordLength = 16
	)

	// Ensure at least one character from each category
	password := make([]byte, passwordLength)

	// Add at least one character from each required group
	addRandomChar := func(charSet string, position int) {
		maxVal := big.NewInt(int64(len(charSet)))
		randomIndex, _ := rand.Int(rand.Reader, maxVal)
		password[position] = charSet[randomIndex.Int64()]
	}

	// Add one of each required character type
	addRandomChar(uppercaseChars, 0)
	addRandomChar(lowercaseChars, 1)
	addRandomChar(numberChars, 2)
	addRandomChar(specialChars, 3)

	// Fill the rest with random characters from all sets
	allChars := uppercaseChars + lowercaseChars + numberChars + specialChars
	for i := 4; i < passwordLength; i++ {
		maxVal := big.NewInt(int64(len(allChars)))
		randomIndex, _ := rand.Int(rand.Reader, maxVal)
		password[i] = allChars[randomIndex.Int64()]
	}

	// Shuffle the password to avoid predictable positions of character types
	for i := passwordLength - 1; i > 0; i-- {
		maxVal := big.NewInt(int64(i + 1))
		j, _ := rand.Int(rand.Reader, maxVal)
		password[i], password[j.Int64()] = password[j.Int64()], password[i]
	}

	return string(password)
}

func newUserPrincipalSyncer(ctx context.Context, c *mssqldb.Client) *userPrincipalSyncer {
	return &userPrincipalSyncer{
		resourceType: resourceTypeUser,
		client:       c,
	}
}
