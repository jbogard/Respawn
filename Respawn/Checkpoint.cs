
using System.Collections;
using Respawn.Graph;

namespace Respawn
{
	using System;
	using System.Collections.Generic;
	using System.Data.Common;
	using Microsoft.Data.SqlClient;
	using System.Linq;
	using System.Threading.Tasks;

	public class Checkpoint
	{
		private GraphBuilder _graphBuilder;
		private IList<TemporalTable> _temporalTables = new List<TemporalTable>();

		public string[] TablesToIgnore { get; set; } = new string[0];
		public string[] TablesToInclude { get; set; } = new string[0];
		public string[] SchemasToInclude { get; set; } = new string[0];
		public string[] SchemasToExclude { get; set; } = new string[0];
		public string DeleteSql { get; private set; }
		public string ReseedSql { get; private set; }
		public bool CheckTemporalTables { get; set; } = false;
		internal string DatabaseName { get; private set; }
		public bool WithReseed { get; set; } = false;
		public IDbAdapter DbAdapter { get; set; } = Respawn.DbAdapter.SqlServer;

		public int? CommandTimeout { get; set; }

		public virtual async Task Reset(string nameOrConnectionString)
		{
			using (var connection = new SqlConnection(nameOrConnectionString))
			{
				await connection.OpenAsync();

				await Reset(connection);
			}
		}

		public virtual async Task Reset(DbConnection connection)
		{
			if (string.IsNullOrWhiteSpace(DeleteSql))
			{
				DatabaseName = connection.Database;
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
			using (var tx = connection.BeginTransaction())
			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandTimeout = CommandTimeout ?? cmd.CommandTimeout;
				cmd.CommandText = commandText;
				cmd.Transaction = tx;

				await cmd.ExecuteNonQueryAsync();

				tx.Commit();
			}
		}

		private async Task ExecuteDeleteSqlAsync(DbConnection connection)
		{
			using (var tx = connection.BeginTransaction())
			using (var cmd = connection.CreateCommand())
			{
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
		}

		private async Task BuildDeleteTables(DbConnection connection)
		{
			var allTables = await GetAllTables(connection);

			if (CheckTemporalTables && DoesDbSupportsTemporalTables(connection))
			{
				_temporalTables = await GetAllTemporalTables(connection);
			}

			var allRelationships = await GetRelationships(connection);

			_graphBuilder = new GraphBuilder(allTables, allRelationships);

			DeleteSql = DbAdapter.BuildDeleteCommandText(_graphBuilder);
			if (WithReseed)
			{
				ReseedSql = DbAdapter.BuildReseedSql(_graphBuilder.ToDelete);
			}
			else
			{
				ReseedSql = null;
			}
		}

		private async Task<HashSet<Relationship>> GetRelationships(DbConnection connection)
		{
			var rels = new HashSet<Relationship>();
			var commandText = DbAdapter.BuildRelationshipCommandText(this);

			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText = commandText;
				using (var reader = await cmd.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						rels.Add(new Relationship(
							reader.IsDBNull(0) ? null : reader.GetString(0),
							reader.GetString(1),
							reader.IsDBNull(2) ? null : reader.GetString(2), 
							reader.GetString(3), 
							reader.GetString(4)));
					}
				}
			}

			return rels;
		}

		private async Task<HashSet<Table>> GetAllTables(DbConnection connection)
		{
			var tables = new HashSet<Table>();

			string commandText = DbAdapter.BuildTableCommandText(this);

			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText = commandText;
				using (var reader = await cmd.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						tables.Add(new Table(reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetString(1)));
					}
				}
			}

			return tables;
		}

		private async Task<IList<TemporalTable>> GetAllTemporalTables(DbConnection connection)
		{
			var tables = new List<TemporalTable>();

			string commandText = DbAdapter.BuildTemporalTableCommandText(this);

			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText = commandText;
				using (var reader = await cmd.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						tables.Add(new TemporalTable(reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetString(1), reader.GetString(2)));
					}
				}
			}

			return tables;
		}

		private bool DoesDbSupportsTemporalTables(DbConnection connection)
		{
			if (DbAdapter.GetType() == Respawn.DbAdapter.SqlServer.GetType())
			{
				const int SqlServer2016MajorBuildVersion = 13;
				string serverVersion = connection.ServerVersion;
				string[] serverVersionDetails = serverVersion.Split(new string[] { "." }, StringSplitOptions.None);
				int versionNumber = int.Parse(serverVersionDetails[0]);
				return versionNumber >= SqlServer2016MajorBuildVersion;
			}
			return false;
		}
	}
}
