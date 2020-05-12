using System;

namespace Particular.TimeoutMigrationTool
{
    class SqlDialect
    {
        internal static SqlDialect Parse(string v)
        {
            return new MsSqlServer();
        }
    }

    class MsSqlServer : SqlDialect { }
    class Oracle : SqlDialect { }
    class MySql : SqlDialect { }
    class PostgreSql : SqlDialect { }
}