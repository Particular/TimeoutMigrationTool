namespace TimeoutMigrationTool.NHibernate.AcceptanceTests
{
    using System;
    using System.IO;
    using Microsoft.Data.SqlClient;
    using global::NHibernate;
    using global::NHibernate.Dialect;
    using global::NHibernate.Driver;
    using global::NHibernate.Mapping.ByCode;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.NHibernate;
    using System.Threading.Tasks;

    class NHibernateAcceptanceTests
    {
        internal string connectionString;

        internal DatabaseDialect DatabaseDialect = new MsSqlDatabaseDialect();

        [SetUp]
        public async Task SetUp()
        {
            var databaseName = $"Att{TestContext.CurrentContext.Test.ID.Replace("-", "")}";
            connectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={databaseName};Integrated Security=True;";

            await RecreateDbIfNotExists(connectionString);
        }

        internal ISessionFactory CreateSessionFactory()
        {
            var cfg = new global::NHibernate.Cfg.Configuration().DataBaseIntegration(x =>
            {
                x.ConnectionString = connectionString;
                x.Driver<MicrosoftDataSqlClientDriver>();
                x.Dialect<MsSql2008Dialect>();
                x.LogSqlInConsole = true;
            });

            var mapper = new ModelMapper();
            mapper.AddMapping<TimeoutEntityMap>();
            mapper.AddMapping<StagedTimeoutEntityMap>();

            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            cfg.AddMapping(mapping);
            cfg.SetProperty("hbm2ddl.auto", "update"); // creates the schema, destroying previous data

            return cfg.BuildSessionFactory();
        }

        internal string GetSqlQueryToLoadBatchState()
        {
            return "SELECT TOP 1 BatchState FROM StagedTimeoutEntity WHERE BatchNumber = 1";
        }

        [SetUp]
        public async Task Setup()
        {
            await DropAllTablesInDatabase();
        }

        async Task DropAllTablesInDatabase()
        {
            // Drop schema before tests
            await using var connection = new SqlConnection(connectionString);
            await using var command = connection.CreateCommand();

            command.CommandText = @"
DECLARE @sql NVARCHAR(max) = ''

SELECT @sql += ' Drop table ' + QUOTENAME(s.NAME) + '.' + QUOTENAME(t.NAME) + '; '
FROM   sys.tables t
       JOIN sys.schemas s
         ON t.[schema_id] = s.[schema_id]
WHERE  t.type = 'U'

Exec sp_executesql @sql
";

            await connection.OpenAsync();
            await command.ExecuteNonQueryAsync();
        }

        static async Task RecreateDbIfNotExists(string connectionString = null)
        {
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

        static async Task DropDatabase(string connectionString, string databaseName)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master" };

            await using var connection = new SqlConnection(connectionStringBuilder.ToString());
            await connection.OpenAsync();
            await using var dropCommand = connection.CreateCommand();
            dropCommand.CommandText = $"use master; if exists(select * from sysdatabases where name = '{databaseName}') begin alter database {databaseName} set SINGLE_USER with rollback immediate; drop database {databaseName}; end; ";
            await dropCommand.ExecuteNonQueryAsync();
        }

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            if (Directory.Exists(StorageRootDir))
            {
                Directory.Delete(StorageRootDir, true);
            }
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            if (Directory.Exists(StorageRootDir))
            {
                Directory.Delete(StorageRootDir, true);
            }
        }

        public static string StorageRootDir
        {
            get
            {
                string tempDir;

                if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                {
                    //can't use bin dir since that will be too long on the build agents
                    tempDir = @"c:\temp";
                }
                else
                {
                    tempDir = Path.GetTempPath();
                }

                return Path.Combine(tempDir, "timeoutmigrationtool-accpt-tests");
            }
        }
    }
}