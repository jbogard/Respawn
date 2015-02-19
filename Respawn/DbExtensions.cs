namespace Respawn
{
    using System;
    using System.Data.Common;
    using System.Globalization;
    using System.Reflection;

    internal static class Db
    {

        static readonly Func<DbConnection, DbProviderFactory> getDbProviderFactory;
        static readonly Func<DbCommandBuilder, int, string> getParameterName;
        static readonly Func<DbCommandBuilder, int, string> getParameterPlaceholder;

        static Db()
        {

            getDbProviderFactory = (Func<DbConnection, DbProviderFactory>)Delegate.CreateDelegate(typeof(Func<DbConnection, DbProviderFactory>), typeof(DbConnection).GetProperty("DbProviderFactory", BindingFlags.Instance | BindingFlags.NonPublic).GetGetMethod(true));
            getParameterName = (Func<DbCommandBuilder, int, string>)Delegate.CreateDelegate(typeof(Func<DbCommandBuilder, int, string>), typeof(DbCommandBuilder).GetMethod("GetParameterName", BindingFlags.Instance | BindingFlags.NonPublic, Type.DefaultBinder, new Type[] { typeof(Int32) }, null));
            getParameterPlaceholder = (Func<DbCommandBuilder, int, string>)Delegate.CreateDelegate(typeof(Func<DbCommandBuilder, int, string>), typeof(DbCommandBuilder).GetMethod("GetParameterPlaceholder", BindingFlags.Instance | BindingFlags.NonPublic, Type.DefaultBinder, new Type[] { typeof(Int32) }, null));
        }

        public static DbProviderFactory GetProviderFactory(this DbConnection connection)
        {
            return getDbProviderFactory(connection);
        }

        public static DbCommand CreateCommand(this DbConnection connection, string commandText, params object[] parameters)
        {

            if (connection == null) throw new ArgumentNullException("connection");

            return CreateCommandImpl(GetProviderFactory(connection).CreateCommandBuilder(), connection.CreateCommand(), commandText, parameters);
        }

        private static DbCommand CreateCommandImpl(DbCommandBuilder commandBuilder, DbCommand command, string commandText, params object[] parameters)
        {

            if (commandBuilder == null) throw new ArgumentNullException("commandBuilder");
            if (command == null) throw new ArgumentNullException("command");
            if (commandText == null) throw new ArgumentNullException("commandText");

            if (parameters == null || parameters.Length == 0)
            {
                command.CommandText = commandText;
                return command;
            }

            object[] paramPlaceholders = new object[parameters.Length];

            for (int i = 0; i < paramPlaceholders.Length; i++)
            {

                DbParameter dbParam = command.CreateParameter();
                dbParam.ParameterName = getParameterName(commandBuilder, i);
                dbParam.Value = parameters[i] ?? DBNull.Value;
                command.Parameters.Add(dbParam);

                paramPlaceholders[i] = getParameterPlaceholder(commandBuilder, i);
            }

            command.CommandText = String.Format(CultureInfo.InvariantCulture, commandText, paramPlaceholders);

            return command;
        }
    }
}