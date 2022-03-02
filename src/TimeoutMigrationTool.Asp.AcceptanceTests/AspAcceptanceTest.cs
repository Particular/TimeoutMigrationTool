namespace TimeoutMigrationTool.Asp.AcceptanceTests
{
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Cosmos.Table;
    using Particular.TimeoutMigrationTool.Asp;
    using NServiceBus;
    using NUnit.Framework;
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using static Microsoft.Azure.Cosmos.Table.TableQuery;

    [TestFixture]
    public abstract class AspAcceptanceTest
    {
        const string TimeoutTableName = "timeoutstable";
        const string PartitionKeyScope = "yyyyMMdd";

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

            tableNamePrefix = $"Att{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}{DateTime.UtcNow.Ticks}".ToLowerInvariant();
            timeoutTableName = tableNamePrefix + TimeoutTableName;
            timeoutContainerName = tableNamePrefix + "timeoutsstate";

            connectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.AzureStorage_ConnectionString);

            var account = CloudStorageAccount.Parse(connectionString);
            tableClient = account.CreateCloudTableClient();


            timeoutTable = tableClient.GetTableReference(timeoutTableName);
            await timeoutTable.CreateIfNotExistsAsync();

            blobServiceClient = new BlobServiceClient(connectionString);
            await blobServiceClient.CreateBlobContainerAsync(timeoutContainerName);
        }

        [TearDown]
        public async Task TearDown()
        {
            foreach (var table in tableClient.ListTables())
            {
                if (table.Name.StartsWith(tableNamePrefix))
                {
                    await table.DeleteAsync();
                }
            }

            await blobServiceClient.DeleteBlobContainerAsync(timeoutContainerName);
        }

        protected void SetupPersistence(EndpointConfiguration endpointConfiguration)
        {
            var persistence = endpointConfiguration.UsePersistence<AzureStoragePersistence>();
            persistence.ConnectionString(connectionString);

            var timeoutPersistence = endpointConfiguration.UsePersistence<AzureStoragePersistence, StorageType.Timeouts>();
#pragma warning disable 618
            timeoutPersistence.CreateSchema(true);
            timeoutPersistence.TimeoutDataTableName(timeoutTableName);
            timeoutPersistence.TimeoutStateContainerName(timeoutContainerName);
            timeoutPersistence.PartitionKeyScope(PartitionKeyScope);
#pragma warning restore 618
        }

        protected AspTimeoutsSource CreateTimeoutStorage(string endpointNameToBeListed, int batchSize = 1024)
        {
            var storage = new AspTimeoutsSource(connectionString, batchSize, timeoutContainerName, endpointNameToBeListed, TimeoutTableName, tablePrefix: tableNamePrefix, partitionKeyScope: PartitionKeyScope);
            return storage;
        }

        protected async Task WaitUntilTheTimeoutsAreSavedInAsp(string endpoint, int numberOfEntriesThatShouldBeThere)
        {
            var query = new TableQuery<DynamicTableEntity>()
                .Where(GenerateFilterCondition("OwningTimeoutManager", QueryComparisons.Equal, $"{endpoint}"));

            TableQuerySegment<DynamicTableEntity> result;
            do
            {
                await Task.Delay(200);

                result = await timeoutTable.ExecuteQuerySegmentedAsync(query, null);
            } while (result?.Results.Count < numberOfEntriesThatShouldBeThere);
        }

        protected CloudTableClient tableClient;
        protected string tableNamePrefix;
        protected string connectionString;
        protected CloudTable timeoutTable;
        string timeoutTableName;
        string timeoutContainerName;
        BlobServiceClient blobServiceClient;

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