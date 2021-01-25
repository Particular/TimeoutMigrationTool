namespace TimeoutMigrationTool.NHibernate.AcceptanceTests
{
    using Microsoft.Data.SqlClient;
    using global::NHibernate;
    using global::NHibernate.Cfg.MappingSchema;
    using global::NHibernate.Dialect;
    using global::NHibernate.Driver;
    using global::NHibernate.Mapping.ByCode;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.NHibernate;
    using System.Threading.Tasks;

    class NHibernateAcceptanceTests
    {
        public NHibernateAcceptanceTests()
        {
            var databaseName = $"Att{TestContext.CurrentContext.Test.ID.Replace("-", "")}";
            connectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={databaseName};Integrated Security=True;";

            RecreateDbIfNotExists(connectionString);
        }

        internal string connectionString;

        internal DatabaseDialect DatabaseDialect = new MsSqlDatabaseDialect();

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

            HbmMapping mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            cfg.AddMapping(mapping);
            cfg.SetProperty("hbm2ddl.auto", "update"); // creates the schema, destroying previous data

            return cfg.BuildSessionFactory();
        }

        internal string GetSqlQueryToLoadBatchState(int batchNumber)
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
            using var connection = new SqlConnection(connectionString);
            using var command = connection.CreateCommand();

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

        public static void RecreateDbIfNotExists(string connectionString = null)
        {
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