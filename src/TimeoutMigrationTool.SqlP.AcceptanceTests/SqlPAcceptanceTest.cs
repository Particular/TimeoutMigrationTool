namespace TimeoutMigrationTool.AcceptanceTests
{
    using Microsoft.Data.SqlClient;
    using NServiceBus;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.SqlP;
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    [TestFixture]
    public abstract class SqlPAcceptanceTest
    {
        [SetUp]
        public Task SetUp()
        {
            NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention = t =>
            {
                var classAndEndpoint = t.FullName.Split('.').Last();

                var testName = classAndEndpoint.Split('+').First();

                testName = testName.Replace("When_", "");

                var endpointBuilder = classAndEndpoint.Split('+').Last();

                testName = Thread.CurrentThread.CurrentCulture.TextInfo.ToTitleCase(testName);

                testName = testName.Replace("_", "");

                return testName + "_" + endpointBuilder;
            };

            databaseName = $"Att{TestContext.CurrentContext.Test.ID.Replace("-", "")}";

            connectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={databaseName};Integrated Security=True;";
            rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";

            MsSqlMicrosoftDataClientHelper.RecreateDbIfNotExists(connectionString);

            return Task.CompletedTask;
        }

        protected void SetupPersitence(EndpointConfiguration endpointConfiguration)
        {
            var persistence = endpointConfiguration.UsePersistence<SqlPersistence>();

            persistence.SqlDialect<NServiceBus.SqlDialect.MsSqlServer>();
            persistence.ConnectionBuilder(
                connectionBuilder: () =>
                {
                    return new SqlConnection(connectionString);
                });
        }

        protected int NumberOfTimeouts(string endpointName)
        {
            return QueryScalar<int>($"SELECT COUNT(*) FROM {endpointName}_TimeoutData");
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

        protected SqlTimeoutStorage GetTimeoutStorage(int batchSize = 1024)
        {
            var storage = new SqlTimeoutStorage(connectionString, new MsSqlServer(), batchSize);

            //TODO: Add a propoer Init()
            storage.TryLoadOngoingMigration().GetAwaiter().GetResult();

            return storage;
        }

        protected string databaseName;
        protected string connectionString;
        protected string rabbitUrl;

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