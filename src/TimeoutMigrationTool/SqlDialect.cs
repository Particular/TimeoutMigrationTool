using System;

namespace Particular.TimeoutMigrationTool
{
    public class SqlDialect
    {
        public static SqlDialect Parse(string dialectString)
        {
            return new MsSqlServer();
        }
    }

    public class MsSqlServer : SqlDialect { }
    public class Oracle : SqlDialect { }
    public class MySql : SqlDialect { }
    public class PostgreSql : SqlDialect { }
}