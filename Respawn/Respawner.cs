using Respawn.Graph;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Respawn
{
    public class Respawner
    {
        private readonly IDbAdapter _dbAdapter;
        private IList<TemporalTable> _temporalTables = new List<TemporalTable>();
        public RespawnerOptions Options { get; }
        public string? DeleteSql { get; private set; }
        public string? ReseedSql { get; private set; }

        private Respawner(RespawnerOptions options, IDbAdapter dbAdapter)
        {
            if (options.DbAdapter != null)
            {
                Options = options;
            }
            else
            {
                Options = new RespawnerOptions
                {
                    TablesToIgnore = options.TablesToIgnore,
                    TablesToInclude = options.TablesToInclude,
                    SchemasToInclude = options.SchemasToInclude,
                    SchemasToExclude = options.SchemasToExclude,
                    CheckTemporalTables = options.CheckTemporalTables,
                    WithReseed = options.WithReseed,
                    CommandTimeout = options.CommandTimeout,
                    DbAdapter = dbAdapter,
                };
            }
            _dbAdapter = dbAdapter;
        }

        /// <summary>
        /// Creates a <see cref="Respawner"/> based on the supplied connection and options.
        /// </summary>
        /// <param name="connection">Connection object for your target database</param>
        /// <param name="options">Options</param>
        /// <returns>A respawner with generated SQL based on the supplied connection object.</returns>
        public static async Task<Respawner> CreateAsync(DbConnection connection, RespawnerOptions? options = default)
        {
            options ??= new RespawnerOptions();

            var dbAdapter = options.DbAdapter ?? connection.GetType().Name switch
            {
                "SqlConnection" => DbAdapter.SqlServer,
                "NpgsqlConnection" => DbAdapter.Postgres,
                "MySqlConnection" => DbAdapter.MySql,
                "OracleConnection" => DbAdapter.Oracle,
                "DB2Connection" or "IfxConnection" => DbAdapter.Informix,
                "SnowflakeDbConnection" => DbAdapter.Snowflake,
                _ => throw new ArgumentException("The database adapter could not be inferred from the DbConnection. Please pass an explicit database adapter in the options.", nameof(options))
            };

            var respawner = new Respawner(options, dbAdapter);

            await respawner.BuildDeleteTables(connection).ConfigureAwait(false);

            return respawner;
        }

        public virtual async Task ResetAsync(DbConnection connection)
        {
            if (_temporalTables.Any())
            {
                var turnOffVersioningCommandText = _dbAdapter.BuildTurnOffSystemVersioningCommandText(_temporalTables);
                await ExecuteAlterSystemVersioningAsync(connection, turnOffVersioningCommandText).ConfigureAwait(false);
            }

            try
            {
                if (Options.DbAdapter.RequiresStatementsToBeExecutedIndividually())
                {
                    await ExecuteDeleteSqlIndividuallyAsync(connection).ConfigureAwait(false);
                }
                else
                {
                    await ExecuteDeleteSqlAsync(connection).ConfigureAwait(false);
                }
            }
            finally
            {
                if (_temporalTables.Any())
                {
                    var turnOnVersioningCommandText = _dbAdapter.BuildTurnOnSystemVersioningCommandText(_temporalTables);
                    await ExecuteAlterSystemVersioningAsync(connection, turnOnVersioningCommandText).ConfigureAwait(false);
                }
            }
        }

        private async Task ExecuteAlterSystemVersioningAsync(DbConnection connection, string commandText)
        {
            await using var tx = await connection.BeginTransactionAsync().ConfigureAwait(false);
            await using var cmd = connection.CreateCommand();

            cmd.CommandTimeout = Options.CommandTimeout ?? cmd.CommandTimeout;
            cmd.CommandText = commandText;
            cmd.Transaction = tx;

            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            await tx.CommitAsync().ConfigureAwait(false);
        }

        private async Task ExecuteDeleteSqlAsync(DbConnection connection)
        {
            await using var tx = await connection.BeginTransactionAsync().ConfigureAwait(false);
            await using var cmd = connection.CreateCommand();

            cmd.CommandTimeout = Options.CommandTimeout ?? cmd.CommandTimeout;
            cmd.CommandText = DeleteSql;
            cmd.Transaction = tx;

            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (ReseedSql != null)
            {
                cmd.CommandText = ReseedSql;
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            await tx.CommitAsync().ConfigureAwait(false);
        }

        private async Task ExecuteDeleteSqlIndividuallyAsync(DbConnection connection)
        {
            await using var tx = await connection.BeginTransactionAsync();
            await using var cmd = connection.CreateCommand();

            var deleteStatements = DeleteSql!.Split([";"], StringSplitOptions.RemoveEmptyEntries);

            foreach (var statement in deleteStatements)
            {
                var trimmedStatement = statement.Trim();

                if (string.IsNullOrWhiteSpace(trimmedStatement))
                {
                    continue;
                }

                cmd.CommandTimeout = Options.CommandTimeout ?? cmd.CommandTimeout;
                cmd.CommandText = trimmedStatement; 
                cmd.Transaction = tx;

                await cmd.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();

            await tx.RollbackAsync();
        }

        private async Task BuildDeleteTables(DbConnection connection)
        {
            var allTables = await GetAllTables(connection).ConfigureAwait(false);

            if (!allTables.Any())
            {
                throw new InvalidOperationException(
                    "No tables found. Ensure your target database has at least one non-ignored table to reset. Consider initializing the database and/or running migrations.");
            }

            if (Options.CheckTemporalTables && await _dbAdapter.CheckSupportsTemporalTables(connection))
            {
                _temporalTables = await GetAllTemporalTables(connection).ConfigureAwait(false);
            }

            var allRelationships = await GetRelationships(connection).ConfigureAwait(false);

            var graphBuilder = new GraphBuilder(allTables, allRelationships);

            DeleteSql = _dbAdapter.BuildDeleteCommandText(graphBuilder, Options);
            ReseedSql = Options.WithReseed ? _dbAdapter.BuildReseedSql(graphBuilder.ToDelete) : null;
        }

        private async Task<HashSet<Relationship>> GetRelationships(DbConnection connection)
        {
            var relationships = new HashSet<Relationship>();
            var commandText = _dbAdapter.BuildRelationshipCommandText(Options);

            await using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                relationships.Add(new Relationship(
                    new Table(await reader.IsDBNullAsync(0).ConfigureAwait(false) ? null : reader.GetString(0), reader.GetString(1)),
                    new Table(await reader.IsDBNullAsync(2).ConfigureAwait(false) ? null : reader.GetString(2), reader.GetString(3)),
                    reader.GetString(4)));
            }

            return relationships;
        }

        private async Task<HashSet<Table>> GetAllTables(DbConnection connection)
        {
            var tables = new HashSet<Table>();

            var commandText = _dbAdapter.BuildTableCommandText(Options);

            await using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                tables.Add(new Table(await reader.IsDBNullAsync(0).ConfigureAwait(false) ? null : reader.GetString(0), reader.GetString(1)));
            }

            return tables;
        }

        private async Task<IList<TemporalTable>> GetAllTemporalTables(DbConnection connection)
        {
            var tables = new List<TemporalTable>();

            var commandText = _dbAdapter.BuildTemporalTableCommandText(Options);

            await using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                tables.Add(new TemporalTable(await reader.IsDBNullAsync(0).ConfigureAwait(false) ? null : reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }

            return tables;
        }
    }
}