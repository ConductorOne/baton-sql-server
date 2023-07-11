# baton-sql-server
`baton-sql-server` is a connector for Microsoft SQL Server. It communicates with the SQL Server to sync data about users, groups, server roles, databases, and database roles.

It uses [go-mssqldb](https://github.com/microsoft/go-mssqldb) to connect to SQL Server. Check out https://github.com/microsoft/go-mssqldb#connection-parameters-and-dsn for more details on how to connect to your server.

Check out [Baton](https://github.com/conductorone/baton) to learn more the project in general.

# Getting Started

## brew

```
brew install conductorone/baton/baton conductorone/baton/baton-sql-server
baton-sql-server --dsn "server=127.0.0.1;user id=sa;password=P@ssw0rd;port=1433" 
baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_DSN="server=127.0.0.1;user id=sa;password=P@ssw0rd;port=1433" ghcr.io/conductorone/baton-sql-server:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-sql-server/cmd/baton-sql-server@main
baton-sql-server --dsn "server=127.0.0.1;user id=sa;password=P@ssw0rd;port=1433" 
baton resources
```

# Data Model

`baton-sql-server` syncs information about the following resources:
- Users
- Groups
- Server Roles
- Databases
- Database Roles

When fetching database permissions, the server principal backing the database principal will the resource that is granted entitlements.

# Contributing, Support, and Issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for everyone. If you have questions, problems, or ideas: Please open a Github Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.

# `baton-sql-server` Command Line Usage

```
baton-sql-server

Usage:
  baton-sql-server [flags]
  baton-sql-server [command]

Available Commands:
  completion         Generate the autocompletion script for the specified shell
  help               Help about any command

Flags:
      --client-id string              The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string          The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
      --dsn string                    The connection string for connecting to SQL Server ($BATON_DSN)
  -f, --file string                   The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                          help for baton-sql-server
      --log-format string             The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string              The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
  -v, --version                       version for baton-sql-server

Use "baton-sql-server [command] --help" for more information about a command.

```