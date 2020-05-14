using System;
using Microsoft.Data.SqlClient;

public static class MsSqlMicrosoftDataClientConnectionBuilder
{
    const string ConnectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=Test;Integrated Security=True;Initial Catalog=Test;";

    public static SqlConnection Build()
    {
        return new SqlConnection(GetConnectionString());
    }

    public static void RecreateDbIfNotExists()
    {
        var connectionStringBuilder = new SqlConnectionStringBuilder(GetConnectionString());
        var databaseName = connectionStringBuilder.InitialCatalog;

        DropDatabase(databaseName);

        connectionStringBuilder.InitialCatalog = "master";

        using (var connection = new SqlConnection(connectionStringBuilder.ToString()))
        {
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"select * from master.dbo.sysdatabases where name='{databaseName}'";
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows) // exists
                    {
                        return;
                    }
                }

                command.CommandText = $"CREATE DATABASE {databaseName} COLLATE SQL_Latin1_General_CP1_CS_AS";
                command.ExecuteNonQuery();
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

    static void DropDatabase(string databaseName)
    {
        var connectionStringBuilder = new SqlConnectionStringBuilder(GetConnectionString());

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
