namespace Respawn
{
    using System;
    using System.Data.Common;
    using System.Globalization;

    internal static class Db
    {
        private static ParameterNameGenerator parameterNameGenerator;

        static Db()
        {
            parameterNameGenerator = new ParameterNameGenerator();
        }

        public static DbCommand CreateCommand(this DbConnection connection, string commandText, params object[] parameters)
        {
            if (connection == null) throw new ArgumentNullException("connection");

            return CreateCommandImpl(connection.CreateCommand(), commandText, parameters);
        }

        private static DbCommand CreateCommandImpl(DbCommand command, string commandText, params object[] parameters)
        {
            if (command == null) throw new ArgumentNullException("command");
            if (commandText == null) throw new ArgumentNullException("commandText");

            if (parameters == null || parameters.Length == 0)
            {
                command.CommandText = commandText;
                return command;
            }

            parameterNameGenerator.Reset();

            object[] paramPlaceholders = new object[parameters.Length];

            for (int i = 0; i < paramPlaceholders.Length; i++)
            {
                var parameterName = parameterNameGenerator.GenerateNext();
                DbParameter dbParam = command.CreateParameter();
                dbParam.ParameterName = parameterName;
                dbParam.Value = parameters[i] ?? DBNull.Value;
                command.Parameters.Add(dbParam);

                paramPlaceholders[i] = parameterName;
            }

            command.CommandText = String.Format(CultureInfo.InvariantCulture, commandText, paramPlaceholders);

            return command;
        }
    }
}