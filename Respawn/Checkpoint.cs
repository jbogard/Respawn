﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Respawn.Graph;

namespace Respawn
{
    public class Checkpoint
    {
        private IList<TemporalTable> _temporalTables = new List<TemporalTable>();

        public Table[] TablesToIgnore { get; init; } = Array.Empty<Table>();
        public Table[] TablesToInclude { get; init; } = Array.Empty<Table>();
        public string[] SchemasToInclude { get; init; } = Array.Empty<string>();
        public string[] SchemasToExclude { get; init; } = Array.Empty<string>();
        public string? DeleteSql { get; private set; }
        public string? ReseedSql { get; private set; }
        public bool CheckTemporalTables { get; init; }
        public bool WithReseed { get; init; }
        public IDbAdapter DbAdapter { get; init; } = Respawn.DbAdapter.SqlServer;

        public int? CommandTimeout { get; init; }

        public virtual async Task Reset(string nameOrConnectionString)
        {
            using var connection = new SqlConnection(nameOrConnectionString);

            await connection.OpenAsync();

            await Reset(connection);
        }

        public virtual async Task Reset(DbConnection connection)
        {
            if (string.IsNullOrWhiteSpace(DeleteSql))
            {
                await BuildDeleteTables(connection);
            }

            if (_temporalTables.Any())
            {
                var turnOffVersioningCommandText = DbAdapter.BuildTurnOffSystemVersioningCommandText(_temporalTables);
                await ExecuteAlterSystemVersioningAsync(connection, turnOffVersioningCommandText);
            }

            await ExecuteDeleteSqlAsync(connection);

            if (_temporalTables.Any())
            {
                var turnOnVersioningCommandText = DbAdapter.BuildTurnOnSystemVersioningCommandText(_temporalTables);
                await ExecuteAlterSystemVersioningAsync(connection, turnOnVersioningCommandText);
            }
        }

        private async Task ExecuteAlterSystemVersioningAsync(DbConnection connection, string commandText)
        {
            using var tx = connection.BeginTransaction();
            using var cmd = connection.CreateCommand();

            cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;
            cmd.CommandText = commandText;
            cmd.Transaction = tx;

            await cmd.ExecuteNonQueryAsync();

            tx.Commit();
        }

        private async Task ExecuteDeleteSqlAsync(DbConnection connection)
        {
            using var tx = connection.BeginTransaction();
            using var cmd = connection.CreateCommand();

            cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;
            cmd.CommandText = DeleteSql;
            cmd.Transaction = tx;

            await cmd.ExecuteNonQueryAsync();

            if (ReseedSql != null)
            {
                cmd.CommandText = ReseedSql;
                await cmd.ExecuteNonQueryAsync();
            }

            tx.Commit();
        }

        private async Task BuildDeleteTables(DbConnection connection)
        {
            var allTables = await GetAllTables(connection);

            if (CheckTemporalTables && DoesDbSupportsTemporalTables(connection))
            {
                _temporalTables = await GetAllTemporalTables(connection);
            }

            var allRelationships = await GetRelationships(connection);

            var graphBuilder = new GraphBuilder(allTables, allRelationships);

            DeleteSql = DbAdapter.BuildDeleteCommandText(graphBuilder);
            ReseedSql = WithReseed ? DbAdapter.BuildReseedSql(graphBuilder.ToDelete) : null;
        }

        private async Task<HashSet<Relationship>> GetRelationships(DbConnection connection)
        {
            var relationships = new HashSet<Relationship>();
            var commandText = DbAdapter.BuildRelationshipCommandText(this);

            using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            using var reader = await cmd.ExecuteReaderAsync();

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

            var commandText = DbAdapter.BuildTableCommandText(this);

            using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                tables.Add(new Table(await reader.IsDBNullAsync(0) ? null : reader.GetString(0), reader.GetString(1)));
            }

            return tables;
        }

        private async Task<IList<TemporalTable>> GetAllTemporalTables(DbConnection connection)
        {
            var tables = new List<TemporalTable>();

            var commandText = DbAdapter.BuildTemporalTableCommandText(this);

            using var cmd = connection.CreateCommand();

            cmd.CommandText = commandText;

            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                tables.Add(new TemporalTable(await reader.IsDBNullAsync(0) ? null : reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }

            return tables;
        }

        private bool DoesDbSupportsTemporalTables(DbConnection connection)
        {
            if (!DbAdapter.SupportsTemporalTables)
                return false;

            const int SqlServer2016MajorBuildVersion = 13;
            var serverVersion = connection.ServerVersion;
            var serverVersionDetails = serverVersion.Split(new[] { "." }, StringSplitOptions.None);
            var versionNumber = int.Parse(serverVersionDetails[0]);
            return versionNumber >= SqlServer2016MajorBuildVersion;
        }
    }
}
