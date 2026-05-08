namespace TimeoutMigrationTool.Asp.AcceptanceTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Cosmos.Table;
    using Newtonsoft.Json;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.Asp;
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

                // A staging table from one framework run that is deleted will cause another attempt to create it to result in Conflict exception
                // for several seconds after the delete occurs, causing a problem for other target framework test runs unless the endpoint
                // names are different
                return $"{testName}-{endpointBuilder}-net{Environment.Version.Major}";
            };

            tableNamePrefix = $"Att{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}{DateTime.UtcNow.Ticks}".ToLowerInvariant();
            timeoutTableName = tableNamePrefix + TimeoutTableName;
            timeoutContainerName = tableNamePrefix + "timeoutsstate";

            connectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.AzureStorageConnectionString);

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

        protected AspTimeoutsSource CreateTimeoutStorage(string endpointNameToBeListed, int batchSize = 1024)
        {
            var storage = new AspTimeoutsSource(connectionString, batchSize, timeoutContainerName, endpointNameToBeListed, TimeoutTableName, tablePrefix: tableNamePrefix, partitionKeyScope: PartitionKeyScope);
            return storage;
        }

        protected async Task StoreLegacyTimeout(string sourceEndpoint, string targetEndpoint, Type messageType)
        {
            var timeoutId = Guid.NewGuid().ToString("N");
            var timeoutTime = DateTime.UtcNow.AddSeconds(5);
            var stateAddress = $"{timeoutId}.state";
            var body = Encoding.UTF8.GetBytes("{}");

            var blobContainerClient = blobServiceClient.GetBlobContainerClient(timeoutContainerName);
            await blobContainerClient.UploadBlobAsync(stateAddress, new BinaryData(body));

            var timeout = new TimeoutDataEntity(timeoutTime.ToString(PartitionKeyScope), timeoutId)
            {
                Destination = targetEndpoint,
                SagaId = Guid.NewGuid(),
                StateAddress = stateAddress,
                Time = timeoutTime,
                OwningTimeoutManager = sourceEndpoint,
                Headers = JsonConvert.SerializeObject(new Dictionary<string, string>
                {
                    { "NServiceBus.ContentType", "application/json" },
                    { "NServiceBus.EnclosedMessageTypes", messageType.AssemblyQualifiedName },
                    { "NServiceBus.MessageId", timeoutId }
                })
            };

            await timeoutTable.ExecuteAsync(TableOperation.Insert(timeout));
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
