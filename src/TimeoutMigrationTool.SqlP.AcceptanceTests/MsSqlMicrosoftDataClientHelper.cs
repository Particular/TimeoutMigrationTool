namespace TimeoutMigrationTool.SqlP.AcceptanceTests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    public static class MsSqlMicrosoftDataClientHelper
    {
        public static async Task RemoveDbIfExists(string connectionString = null)
        {
            connectionString ??= GetConnectionString();

            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var databaseName = connectionStringBuilder.InitialCatalog;

            await DropDatabase(connectionString, databaseName);
        }

        public static async Task RecreateDbIfNotExists(string connectionString = null)
        {
            connectionString ??= GetConnectionString();

            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var databaseName = connectionStringBuilder.InitialCatalog;

            await DropDatabase(connectionString, databaseName);

            connectionStringBuilder.InitialCatalog = "master";

            await using var connection = new SqlConnection(connectionStringBuilder.ToString());
            await connection.OpenAsync();

            await using var command = connection.CreateCommand();
            command.CommandText = $"select * from master.dbo.sysdatabases where name='{databaseName}'";
            await using (var reader = await command.ExecuteReaderAsync())
            {
                if (reader.HasRows)
                {
                    return;
                }
            }

            command.CommandText = $"CREATE DATABASE {databaseName} COLLATE SQL_Latin1_General_CP1_CS_AS";
            await command.ExecuteNonQueryAsync();
        }

        public static string GetConnectionString() =>
            Environment.GetEnvironmentVariable(EnvironmentVariables.SqlServerConnectionString);

        static async Task DropDatabase(string connectionString, string databaseName)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master" };

            await using var connection = new SqlConnection(connectionStringBuilder.ToString());
            await connection.OpenAsync();
            await using var dropCommand = connection.CreateCommand();
            dropCommand.CommandText = $"use master; if exists(select * from sysdatabases where name = '{databaseName}') begin alter database {databaseName} set SINGLE_USER with rollback immediate; drop database {databaseName}; end; ";
            await dropCommand.ExecuteNonQueryAsync();
        }
    }
}