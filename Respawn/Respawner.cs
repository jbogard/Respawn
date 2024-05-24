﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn
{
    public class Respawner
    {
        private IList<TemporalTable> _temporalTables = new List<TemporalTable>();
        public RespawnerOptions Options { get; }
        public string? DeleteSql { get; private set; }
        public string? ReseedSql { get; private set; }

        private Respawner(RespawnerOptions options)
        {
            Options = options;
        }

        /// <summary>
        /// Creates a <see cref="Respawner" /> based on the supplied options and connection string or name. This overload only supports SQL Server.
        /// </summary>
        /// <param name="nameOrConnectionString">Name or connection string</param>
        /// <param name="options">Options</param>
        /// <returns>A respawner with generated SQL based on the supplied connection string</returns>
        /// <exception cref="ArgumentException">Throws if the options are any other database adapter besides SQL</exception>
        public static async Task<Respawner> CreateAsync(string nameOrConnectionString, RespawnerOptions? options = default)
        {
            options ??= new RespawnerOptions();

            if (options.DbAdapter is not SqlServerDbAdapter)
            {
                throw new ArgumentException("This overload only supports the SqlDataAdapter. To use an alternative adapter, use the overload that supplies a DbConnection.", nameof(options.DbAdapter));
            }

#if NETSTANDARD2_0
            using var connection = new SqlConnection(nameOrConnectionString);

            connection.Open();
#else
            await using var connection = new SqlConnection(nameOrConnectionString);

            await connection.OpenAsync();
#endif

            var respawner = new Respawner(options);

            await respawner.BuildDeleteTables(connection);

            return respawner;
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

            var respawner = new Respawner(options);

            await respawner.BuildDeleteTables(connection);

            return respawner;
        }


        public virtual async Task ResetAsync(string nameOrConnectionString)
        {
#if NETSTANDARD2_0
            using var connection = new SqlConnection(nameOrConnectionString);

            connection.Open();
#else
            await using var connection = new SqlConnection(nameOrConnectionString);

            await connection.OpenAsync();
#endif

            await ResetAsync(connection);
        }

        public virtual async Task ResetAsync(DbConnection connection)
        {
            if (_temporalTables.Any())
            {
                var turnOffVersioningCommandText = Options.DbAdapter.BuildTurnOffSystemVersioningCommandText(_temporalTables);
                await ExecuteAlterSystemVersioningAsync(connection, turnOffVersioningCommandText);
            }

            try
            {
                await ExecuteDeleteSqlAsync(connection);
            }
            finally
            {
                if (_temporalTables.Any())
                {
                    var turnOnVersioningCommandText = Options.DbAdapter.BuildTurnOnSystemVersioningCommandText(_temporalTables);
                    await ExecuteAlterSystemVersioningAsync(connection, turnOnVersioningCommandText);
                }
            }
        }

        private async Task ExecuteAlterSystemVersioningAsync(DbConnection connection, string commandText)
        {
#if NETSTANDARD2_0
            using var tx = connection.BeginTransaction();
            using var cmd = connection.CreateCommand();
#else
            await using var tx = await connection.BeginTransactionAsync();
            await using var cmd = connection.CreateCommand();
#endif

            cmd.CommandTimeout = Options.CommandTimeout ?? cmd.CommandTimeout;
            cmd.CommandText = commandText;
            cmd.Transaction = tx;

            await cmd.ExecuteNonQueryAsync();

#if NETSTANDARD2_0
            tx.Commit();
#else
            await tx.CommitAsync();
#endif
        }

        private async Task ExecuteDeleteSqlAsync(DbConnection connection)
        {
#if NETSTANDARD2_0
            using var tx = connection.BeginTransaction();
            using var cmd = connection.CreateCommand();
#else
            await using var tx = await connection.BeginTransactionAsync();
            await using var cmd = connection.CreateCommand();
#endif

            cmd.CommandTimeout = Options.CommandTimeout ?? cmd.CommandTimeout;
            cmd.CommandText = DeleteSql;
            cmd.Transaction = tx;

            await cmd.ExecuteNonQueryAsync();

            if (ReseedSql != null)
            {
                cmd.CommandText = ReseedSql;
                await cmd.ExecuteNonQueryAsync();
            }

#if NETSTANDARD2_0
            tx.Commit();
#else
            await tx.CommitAsync();
#endif
        }

        private async Task BuildDeleteTables(DbConnection connection)
        {
            var allTables = await GetAllTables(connection);

            if (!allTables.Any())
            {
                throw new InvalidOperationException(
                    "No tables found. Ensure your target database has at least one non-ignored table to reset. Consider initializing the database and/or running migrations.");
            }

            if (Options.CheckTemporalTables && await Options.DbAdapter.CheckSupportsTemporalTables(connection))
            {
                _temporalTables = await GetAllTemporalTables(connection);
            }

            var allRelationships = await GetRelationships(connection);

            var graphBuilder = new GraphBuilder(allTables, allRelationships);

            DeleteSql = Options.DbAdapter.BuildDeleteCommandText(graphBuilder);
            ReseedSql = Options.WithReseed ? Options.DbAdapter.BuildReseedSql(graphBuilder.ToDelete) : null;
        }

        private async Task<HashSet<Relationship>> GetRelationships(DbConnection connection)
        {
            var relationships = new HashSet<Relationship>();
            var commandText = Options.DbAdapter.BuildRelationshipCommandText(Options);

#if NETSTANDARD2_0
            using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            using var reader = cmd.ExecuteReader();
#else
            await using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            await using var reader = await cmd.ExecuteReaderAsync();
#endif

            while (await reader.ReadAsync())
            {
                relationships.Add(new Relationship(
                    new Table(await reader.IsDBNullAsync(0) ? null : reader.GetString(0), reader.GetString(1)),
                    new Table(await reader.IsDBNullAsync(2) ? null : reader.GetString(2), reader.GetString(3)),
                    reader.GetString(4)));
            }

            return relationships;
        }

        private async Task<HashSet<Table>> GetAllTables(DbConnection connection)
        {
            var tables = new HashSet<Table>();

            var commandText = Options.DbAdapter.BuildTableCommandText(Options);

#if NETSTANDARD2_0
            using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            using var reader = cmd.ExecuteReader();
#else
            await using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            await using var reader = await cmd.ExecuteReaderAsync();
#endif

            while (await reader.ReadAsync())
            {
                tables.Add(new Table(await reader.IsDBNullAsync(0) ? null : reader.GetString(0), reader.GetString(1)));
            }

            return tables;
        }

        private async Task<IList<TemporalTable>> GetAllTemporalTables(DbConnection connection)
        {
            var tables = new List<TemporalTable>();

            var commandText = Options.DbAdapter.BuildTemporalTableCommandText(Options);

#if NETSTANDARD2_0
            using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            using var reader = cmd.ExecuteReader();
#else
            await using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            await using var reader = await cmd.ExecuteReaderAsync();
#endif

            while (await reader.ReadAsync())
            {
                tables.Add(new TemporalTable(await reader.IsDBNullAsync(0) ? null : reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }

            return tables;
        }
    }
}
