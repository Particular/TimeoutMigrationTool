using Microsoft.Data.SqlClient;
using System;
using System.Data.Common;

namespace Particular.TimeoutMigrationTool
{
    public abstract class SqlDialect
    {
        public abstract DbConnection Connect(string connectionString);

        public static SqlDialect Parse(string dialectString)
        {
            return new MsSqlServer();
        }
    }

    public class MsSqlServer : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();

            return connection;
        }
    }

    public class Oracle : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            throw new NotImplementedException();
        }
    }

    public class MySql : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            throw new NotImplementedException();
        }
    }

    public class PostgreSql : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            throw new NotImplementedException();
        }
    }
}