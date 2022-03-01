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
    using System.IO;
    using System.Threading.Tasks;

    class NHibernateAcceptanceTests
    {
        public NHibernateAcceptanceTests()
        {
            connectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.OracleConnectionString);

            RecreateDbIfNotExists();
        }

        internal DatabaseDialect DatabaseDialect = new OracleDatabaseDialect();

        internal ISessionFactory CreateSessionFactory()
        {
            Console.WriteLine("Connection string in SessionFactory: " + connectionString);
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

        async Task DropAllTablesInDatabase()
        {
            using var session = CreateSessionFactory().OpenSession();

            await DropTable(session, "TIMEOUTENTITY");
            await DropTable(session, "STAGEDTIMEOUTENTITY");
            await DropTable(session, "MIGRATIONSENTITY");
        }

        void RecreateDbIfNotExists()
        {
        }

        async Task DropTable(ISession session, string tableName)
        {
            await session.CreateSQLQuery(@$"BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE {tableName}';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;").ExecuteUpdateAsync();
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

        internal string connectionString;
    }
}