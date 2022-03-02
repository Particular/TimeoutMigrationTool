namespace TimeoutMigrationTool.SqlP.AcceptanceTests
{
    using NServiceBus;
    using NUnit.Framework;
    using System;
    using System.Data.SqlClient;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.SqlServerConnectionString)]
    public abstract class SqlPAcceptanceTest
    {
        [SetUp]
        public async Task SetUp()
        {
            NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention = t =>
            {
                var classAndEndpoint = t.FullName.Split('.').Last();

                var testName = classAndEndpoint.Split('+').First();

                testName = testName.Replace("When_", "");

                var endpointBuilder = classAndEndpoint.Split('+').Last();

                testName = Thread.CurrentThread.CurrentCulture.TextInfo.ToTitleCase(testName);

                testName = testName.Replace("_", "");

                return testName + "-" + endpointBuilder;
            };
            connectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.SqlServerConnectionString);
            databaseName = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
            await MsSqlMicrosoftDataClientHelper.RecreateDbIfNotExists(connectionString);
        }

        [TearDown]
        public async Task TearDown()
        {
            await MsSqlMicrosoftDataClientHelper.RemoveDbIfExists(connectionString);
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

        protected void SetupPersistence(EndpointConfiguration endpointConfiguration)
        {
            var persistence = endpointConfiguration.UsePersistence<SqlPersistence>();

            persistence.SqlDialect<SqlDialect.MsSqlServer>();
            persistence.ConnectionBuilder(
                connectionBuilder: () =>
                {
                    return new SqlConnection(connectionString);
                });
        }

        protected int NumberOfTimeouts(string endpointName)
        {
            return QueryScalar<int>($"SELECT COUNT(*) FROM [{endpointName}_TimeoutData]");
        }

        protected async Task<T> QueryScalarAsync<T>(string sqlStatement)
        {
            using (var connection = MsSqlMicrosoftDataClientHelper.Build(connectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlStatement;

                    return (T)await command.ExecuteScalarAsync();
                }
            }
        }

        protected T QueryScalar<T>(string sqlStatement)
        {
            using (var connection = MsSqlMicrosoftDataClientHelper.Build(connectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlStatement;

                    return (T)command.ExecuteScalar();
                }
            }
        }

        protected async Task WaitUntilTheTimeoutIsSavedInSql(string endpoint)
        {
            while (true)
            {
                var numberOfTimeouts = await QueryScalarAsync<int>($"SELECT COUNT(*) FROM [{endpoint}_TimeoutData]").ConfigureAwait(false);

                if (numberOfTimeouts > 0)
                {
                    return;
                }
            }
        }

        protected string databaseName;
        protected string connectionString;
    }
}