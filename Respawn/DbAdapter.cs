﻿namespace Respawn
{
    public static class DbAdapter
    {
        public static readonly IDbAdapter SqlServer = new SqlServerDbAdapter();
        public static readonly IDbAdapter Postgres = new PostgresDbAdapter();
        public static readonly IDbAdapter MySql = new MySqlAdapter();
        public static readonly IDbAdapter Oracle = new OracleDbAdapter();
        public static readonly IDbAdapter Informix = new InformixDbAdapter();
        public static readonly IDbAdapter DB2 = new DB2DbAdapter();
        public static readonly IDbAdapter Snowflake = new SnowflakeDbAdapter();
    }
}
