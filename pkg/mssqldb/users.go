package mssqldb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

const (
	UserType         = "user"
	DatabaseUserType = "database-user"
)

var ErrNoServerPrincipal = errors.New("no server principal found")

type UserModel struct {
	ID         string `db:"principal_id"`
	SecurityID string `db:"sid"`
	Name       string `db:"name"`
	Type       string `db:"type_desc"`
	IsDisabled bool   `db:"is_disabled"`
}

type UserDBModel struct {
	ID                  string `db:"principal_id"`
	DatabasePrincipalId string `db:"database_principal_id"`
	Sid                 string `db:"sid"`
	Name                string `db:"name"`
	Type                string `db:"type_desc"`
	CreateDate          string `db:"create_date"`
	ModifyDate          string `db:"modify_date"`
	OwningPrincipalId   string `db:"owning_principal_id"`
}

func (c *Client) ListServerUserPrincipals(ctx context.Context, pager *Pager) ([]*UserModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing user principals")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	// Fetch the user principals.
	// https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-server-principals-transact-sql
	_, _ = sb.WriteString(`
SELECT 
  principal_id,
  sid,
  name, 
  type_desc,
  is_disabled
FROM 
  sys.server_principals
WHERE 
  (
    type = 'S' 
    OR type = 'U' 
    OR type = 'C' 
    or type = 'E' 
    or type = 'K'
  ) 
ORDER BY 
  principal_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY
`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*UserModel
	for rows.Next() {
		var userModel UserModel
		err = rows.StructScan(&userModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &userModel)
	}
	if rows.Err() != nil {
		return nil, "", rows.Err()
	}

	var nextPageToken string
	if len(ret) > limit {
		offset += limit
		nextPageToken = strconv.Itoa(offset)
		ret = ret[:limit]
	}

	return ret, nextPageToken, nil
}

// GetServerPrincipalForDatabasePrincipal returns the server principal for a given database user.
// Returns ErrNoServerPrincipal if no server principal is found.
func (c *Client) GetServerPrincipalForDatabasePrincipal(ctx context.Context, dbName string, principalID int64) (*UserModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("getting server principal for database user")

	var sb strings.Builder
	_, _ = sb.WriteString(`
SELECT
	principal_id,
	sid,
	name,
	type_desc,
	is_disabled
FROM
    sys.server_principals 
WHERE sid = (SELECT sid FROM [`)
	_, _ = sb.WriteString(dbName)
	_, _ = sb.WriteString(`].sys.database_principals WHERE principal_id = @p1)`)

	row := c.db.QueryRowxContext(ctx, sb.String(), principalID)
	if row.Err() != nil {
		return nil, row.Err()
	}

	var ret UserModel
	err := row.StructScan(&ret)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoServerPrincipal
		}
		return nil, err
	}

	return &ret, nil
}

func (c *Client) ListDatabaseUserPrincipals(ctx context.Context, dbName string, pager *Pager) ([]*UserModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing database user principals")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	args := []interface{}{offset, limit + 1}

	var sb strings.Builder
	_, _ = sb.WriteString(`
SELECT 
  principal_id,
  name, 
  type_desc
FROM [`)
	_, _ = sb.WriteString(dbName)
	_, _ = sb.WriteString(`].sys.database_principals
WHERE 
  (
    type = 'S' 
    OR type = 'U' 
    OR type = 'C' 
    or type = 'E' 
    or type = 'K'
  ) 
ORDER BY 
  principal_id ASC OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY
`)

	rows, err := c.db.QueryxContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var ret []*UserModel
	for rows.Next() {
		var userModel UserModel
		err = rows.StructScan(&userModel)
		if err != nil {
			return nil, "", err
		}
		ret = append(ret, &userModel)
	}
	if rows.Err() != nil {
		return nil, "", rows.Err()
	}

	var nextPageToken string
	if len(ret) > limit {
		offset += limit
		nextPageToken = strconv.Itoa(offset)
		ret = ret[:limit]
	}

	return ret, nextPageToken, nil
}

func (c *Client) GetUserPrincipal(ctx context.Context, userId string) (*UserModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("getting user")

	query := `
SELECT
    principal_id,
    sid,
    name,
    type_desc,
    is_disabled
FROM
    sys.server_principals
WHERE
    (
		type = 'S'
		OR type = 'U'
		OR type = 'C'
		OR type = 'E'
		OR type = 'K'
	) AND principal_id = @p1
`

	rows := c.db.QueryRowxContext(ctx, query, userId)

	var userModel UserModel
	err := rows.StructScan(&userModel)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("user not found: %s", userId)
		}
		return nil, err
	}

	return &userModel, nil
}

// GetUserFromDb find db user from Server principal.
func (c *Client) GetUserFromDb(ctx context.Context, db, principalId string) (*UserDBModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("getting user")

	if strings.ContainsAny(db, "[]\"';") {
		return nil, fmt.Errorf("invalid characters in dbName")
	}

	query := `
USE [%s];
SELECT
    dp.principal_id AS principal_id,
    sp.principal_id AS database_principal_id,
	dp.sid AS sid,
	dp.name as name,
	dp.type_desc AS type_desc,
	dp.create_date AS create_date,
	dp.modify_date AS modify_date,
	dp.owning_principal_id as owning_principal_id
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp
ON dp.sid = sp.sid
WHERE dp.type IN ('S', 'U')
AND dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')
AND sp.principal_id = @p1
`

	query = fmt.Sprintf(query, db)

	row := c.db.QueryRowxContext(ctx, query, principalId)

	var userModel UserDBModel
	err := row.StructScan(&userModel)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			l.Info("user not found for principal", zap.String("principalId", principalId))
			return nil, nil
		}
		return nil, err
	}

	return &userModel, nil
}

func (c *Client) CreateDatabaseUserForPrincipal(ctx context.Context, db, principal string) error {
	l := ctxzap.Extract(ctx)
	l.Debug("creating user for db user", zap.String("db", db), zap.String("principal", principal))

	if strings.ContainsAny(db, "[]\"';") || strings.ContainsAny(principal, "[]\"';") {
		return fmt.Errorf("invalid characters in dbName or principal")
	}

	query := `
USE [%s];
CREATE USER [%s] FOR LOGIN [%s];
`

	query = fmt.Sprintf(query, db, principal, principal)

	l.Debug("SQL QUERY", zap.String("q", query))

	_, err := c.db.ExecContext(ctx, query)

	if err != nil {
		return err
	}

	return nil
}

// LoginType represents the SQL Server login type.
type LoginType string

const (
	// LoginTypeWindows represents Windows authentication.
	LoginTypeWindows LoginType = "WINDOWS"
	// LoginTypeSQL represents SQL Server authentication.
	LoginTypeSQL LoginType = "SQL"
	// LoginTypeAzureAD represents Azure AD authentication.
	LoginTypeAzureAD LoginType = "AZURE_AD"
	// LoginTypeEntraID represents Azure Entra ID authentication.
	LoginTypeEntraID LoginType = "ENTRA_ID"
)

// CreateLogin creates a SQL Server login with the specified authentication type.
// For Windows authentication (loginType=WINDOWS):
//   - If domain is provided, it will create the login in the format [DOMAIN\Username]
//   - otherwise it will use just [Username]
//
// For SQL authentication (loginType=SQL):
//   - It requires a password
//   - Domain is ignored
//
// For Azure AD authentication (loginType=AZURE_AD):
//   - It creates from EXTERNAL PROVIDER
//   - Username should be the full Azure AD username/email
//
// For Entra ID authentication (loginType=ENTRA_ID):
//   - It creates from EXTERNAL PROVIDER
//   - Username should be the full Entra ID username/email
func (c *Client) CreateLogin(ctx context.Context, loginType LoginType, domain, username, password string) error {
	l := ctxzap.Extract(ctx)

	// Check for invalid characters to prevent SQL injection
	if (domain != "" && strings.ContainsAny(domain, "[]\"';")) || strings.ContainsAny(username, "[]\"';") {
		return fmt.Errorf("invalid characters in domain or username")
	}

	var query string
	switch loginType {
	case LoginTypeWindows:
		var loginName string
		if domain != "" {
			loginName = fmt.Sprintf("[%s\\%s]", domain, username)
			l.Debug("creating windows login with domain", zap.String("login", loginName))
		} else {
			loginName = fmt.Sprintf("[%s]", username)
			l.Debug("creating windows login without domain", zap.String("login", loginName))
		}
		query = fmt.Sprintf("CREATE LOGIN %s FROM WINDOWS;", loginName)
	case LoginTypeSQL:
		if password == "" {
			return fmt.Errorf("password is required for SQL Server authentication")
		}
		// For SQL Server authentication, only username and password are used
		loginName := fmt.Sprintf("[%s]", username)
		l.Debug("creating SQL login", zap.String("login", loginName))
		query = fmt.Sprintf("CREATE LOGIN %s WITH PASSWORD = '%s';", loginName, password)
	case LoginTypeAzureAD, LoginTypeEntraID:
		// Azure AD and Entra ID use external provider
		loginName := fmt.Sprintf("[%s]", username)
		l.Debug("creating external provider login", zap.String("login", loginName), zap.String("type", string(loginType)))
		query = fmt.Sprintf("CREATE LOGIN %s FROM EXTERNAL PROVIDER;", loginName)
	default:
		return fmt.Errorf("unsupported login type: %s", loginType)
	}

	l.Debug("SQL QUERY", zap.String("q", query))

	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create login: %w", err)
	}

	return nil
}

// CreateWindowsLogin creates a SQL Server login from Windows AD for the specified domain and username.
// If domain is provided, it will create the login in the format [DOMAIN\Username],
// otherwise it will use just [Username].
// This is a convenience method that calls CreateLogin with LoginTypeWindows.
func (c *Client) CreateWindowsLogin(ctx context.Context, domain, username string) error {
	return c.CreateLogin(ctx, LoginTypeWindows, domain, username, "")
}
