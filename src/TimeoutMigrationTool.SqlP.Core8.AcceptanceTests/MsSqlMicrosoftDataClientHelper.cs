namespace TimeoutMigrationTool.SqlP.Core8.AcceptanceTests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    public static class MsSqlMicrosoftDataClientHelper
    {
        const string ConnectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=Test;Integrated Security=True;Initial Catalog=Test;";

        public static SqlConnection Build(string connectionString = null)
        {
            if (connectionString == null)
            {
                connectionString = ConnectionString;
            }

            return new SqlConnection(connectionString);
        }

        public static void RecreateDbIfNotExists(string connectionString = null)
        {
            if (connectionString == null)
            {
                connectionString = GetConnectionString();
            }

            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var databaseName = connectionStringBuilder.InitialCatalog;

            DropDatabase(connectionString, databaseName);

            connectionStringBuilder.InitialCatalog = "master";

            using (var connection = new SqlConnection(connectionStringBuilder.ToString()))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"select * from master.dbo.sysdatabases where name='{databaseName}'";
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows) 
                        {
                            return;
                        }
                    }

                    command.CommandText = $"CREATE DATABASE {databaseName} COLLATE SQL_Latin1_General_CP1_CS_AS";
                    command.ExecuteNonQuery();
                }
            }
        }

        public static async Task<T> QueryScalarAsync<T>(string sqlStatement)
        {
            using (var connection = Build())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlStatement;

                    return (T)await command.ExecuteScalarAsync().ConfigureAwait(false);
                }
            }
        }

        public static T QueryScalar<T>(string sqlStatement)
        {
            using (var connection = Build())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlStatement;

                    return (T)command.ExecuteScalar();
                }
            }
        }

        public static string GetConnectionString()
        {
            var connection = Environment.GetEnvironmentVariable("SQLServerConnectionString");

            if (string.IsNullOrWhiteSpace(connection))
            {
                return ConnectionString;
            }

            return connection;
        }

        static void DropDatabase(string connectionString, string databaseName)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);

            connectionStringBuilder.InitialCatalog = "master";

            using (var connection = new SqlConnection(connectionStringBuilder.ToString()))
            {
                connection.Open();
                using (var dropCommand = connection.CreateCommand())
                {
                    dropCommand.CommandText = $"use master; if exists(select * from sysdatabases where name = '{databaseName}') begin alter database {databaseName} set SINGLE_USER with rollback immediate; drop database {databaseName}; end; ";
                    dropCommand.ExecuteNonQuery();
                }
            }
        }
    }
}