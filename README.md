# Respawn

![CI](https://github.com/jbogard/Respawn/workflows/CI/badge.svg)
[![NuGet](https://img.shields.io/nuget/dt/respawn.svg)](https://www.nuget.org/packages/respawn) 
[![NuGet](https://img.shields.io/nuget/vpre/respawn.svg)](https://www.nuget.org/packages/respawn)
[![MyGet (dev)](https://img.shields.io/myget/respawn-ci/v/respawn.svg)](https://myget.org/gallery/respawn-ci)

Respawn is a small utility to help in resetting test databases to a clean state. Instead of deleting data at the end of a test or rolling back a transaction, Respawn [resets the database back to a clean checkpoint](http://lostechies.com/jimmybogard/2013/06/18/strategies-for-isolating-the-database-in-tests/) by intelligently deleting data from tables.

To use, create a `Checkpoint` and initialize with tables you want to skip, or schemas you want to keep/ignore:

```csharp
private static Checkpoint checkpoint = new Checkpoint
{
    TablesToIgnore = new[]
    {
        "sysdiagrams",
        "tblUser",
        "tblObjectType",
    },
    SchemasToExclude = new []
    {
        "RoundhousE"
    }
};
```
Or if you want to use a different database:
```csharp
private static Checkpoint checkpoint = new Checkpoint
{
    SchemasToInclude = new []
    {
        "public"
    },
    DbAdapter = DbAdapter.Postgres
};
```

In your tests, in the fixture setup, reset your checkpoint:
```csharp
await checkpoint.Reset("MyConnectionStringName");
```
or if you're using a database besides SQL Server, pass an open `DbConnection`:
```csharp
using (var conn = new NpgsqlConnection("ConnectionString"))
{
    await conn.OpenAsync();

    await checkpoint.Reset(conn);
}
```

## How does it work?
Respawn examines the SQL metadata intelligently to build a deterministic order of tables to delete based on foreign key relationships between tables. It navigates these relationships to build a DELETE script starting with the tables with no relationships and moving inwards until all tables are accounted for.

Once this in-order list of tables is created, the Checkpoint object keeps this list of tables privately so that the list of tables and the order is only calculated once.

In your tests, you Reset your checkpoint before each test run. If there are any tables/schemas that you don't want to be cleared out, include these in the configuration of your Checkpoint.

In benchmarks, a deterministic deletion of tables is faster than truncation, since truncation requires disabling or deleting foreign key constraints. Deletion results in easier test debugging/maintenance, as transaction rollbacks/post-test deletion still rely on that mechanism at the beginning of each test. If data comes in from another source, your test might fail. Respawning to your checkpoint assures you have a known starting point before each test.

### Installing Respawn

You should install [Respawn with NuGet](https://www.nuget.org/packages/Respawn):

    Install-Package Respawn

Or via the .NET Core CLI:

    dotnet add package Respawn

This command from Package Manager Console will download and install Respawn.

### Local development

To install and run local dependencies needed for tests, (PostgreSQL and MySQL) install Docker for Windows and from the command line at the solution root run:

```
docker-compose up -d
```

This will pull down the latest container images and run them. You can then run the local build/tests.
