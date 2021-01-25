namespace TimeoutMigrationTool.NHibernate.AcceptanceTests
{
    using global::NHibernate;
    using global::NHibernate.Cfg.MappingSchema;
    using global::NHibernate.Dialect;
    using global::NHibernate.Driver;
    using global::NHibernate.Mapping.ByCode;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.NHibernate;
    using System;
    using System.Threading.Tasks;

    class NHibernateAcceptanceTests
    {
        public NHibernateAcceptanceTests()
        {
            connectionString = Environment.GetEnvironmentVariable("OracleConnectionString") ?? $@"Data Source = (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = 127.0.0.1)(PORT = 1521)))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = ORCLCDB.localdomain))); DBA Privilege = SYSDBA; User Id = sys; Password = Oradoc_db1; Enlist = dynamic";

            RecreateDbIfNotExists(connectionString);
        }

        internal DatabaseDialect DatabaseDialect = new OracleDatabaseDialect();

        internal string GetSqlQueryToLoadBatchState(int batchNumber)
        {
            return $"SELECT CAST (BatchState AS INT) FROM StagedTimeoutEntity WHERE BatchNumber = {batchNumber} FETCH NEXT 1 ROWS ONLY";
        }

        internal ISessionFactory CreateSessionFactory()
        {
            var cfg = new global::NHibernate.Cfg.Configuration().DataBaseIntegration(x =>
            {
                x.ConnectionString = connectionString;
                x.Driver<OracleManagedDataClientDriver>();
                x.Dialect<Oracle10gDialect>();
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

        [SetUp]
        public async Task Setup()
        {
            await DropAllTablesInDatabase();
        }

        private async Task DropAllTablesInDatabase()
        {
            using var session = CreateSessionFactory().OpenSession();

            await session.CreateSQLQuery(@"drop table TIMEOUTENTITY cascade constraints").ExecuteUpdateAsync();
            await session.CreateSQLQuery(@"drop table STAGEDTIMEOUTENTITY cascade constraints").ExecuteUpdateAsync();
            await session.CreateSQLQuery(@"drop table MIGRATIONSENTITY cascade constraints").ExecuteUpdateAsync();
        }

        private void RecreateDbIfNotExists(string connectionString)
        {
        }

        internal string connectionString;
    }
}