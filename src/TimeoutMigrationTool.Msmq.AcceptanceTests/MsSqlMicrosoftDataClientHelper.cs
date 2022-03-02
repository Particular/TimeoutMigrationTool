namespace TimeoutMigrationTool.SqlP.AcceptanceTests
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;

    public static class MsSqlMicrosoftDataClientHelper
    {
        public static async Task RemoveDbIfExists(string connectionString = null)
        {
            connectionString = connectionString ?? GetConnectionString();

            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var databaseName = connectionStringBuilder.InitialCatalog;

            await DropDatabase(connectionString, databaseName);
        }

        public static async Task RecreateDbIfNotExists(string connectionString = null)
        {
            connectionString = connectionString ?? GetConnectionString();

            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var databaseName = connectionStringBuilder.InitialCatalog;

            await DropDatabase(connectionString, databaseName);

            connectionStringBuilder.InitialCatalog = "master";

            using (var connection = new SqlConnection(connectionStringBuilder.ToString()))
            {
                await connection.OpenAsync();

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
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public static string GetConnectionString() =>
            Environment.GetEnvironmentVariable(EnvironmentVariables.SqlServerConnectionString);

        static async Task DropDatabase(string connectionString, string databaseName)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master" };

            using (var connection = new SqlConnection(connectionStringBuilder.ToString()))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                using (var dropCommand = connection.CreateCommand())
                {
                    dropCommand.CommandText = $"use master; if exists(select * from sysdatabases where name = '{databaseName}') begin alter database {databaseName} set SINGLE_USER with rollback immediate; drop database {databaseName}; end; ";
                    await dropCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }
    }
}