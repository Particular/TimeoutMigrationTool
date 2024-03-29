﻿namespace TimeoutMigrationTool.Asp.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Cosmos.Table;
    using Newtonsoft.Json;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.Asp;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.AzureStorageConnectionString)]
    public class AspTimeoutsSourceTests
    {
        static readonly string connectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.AzureStorageConnectionString);
        string tableNamePrefix;
        CloudTableClient tableClient;
        BlobContainerClient blobContainerClient;
        BlobServiceClient blobServiceClient;
        string containerName;
        Random random;
        const string fakeEndpointName = "fakeEndpoint";
        static readonly string fakeEndpointTimeoutTableName = $"{fakeEndpointName}timeouts";

        [SetUp]
        public async Task SetUp()
        {
            random = new Random();

            tableNamePrefix = $"{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}{DateTime.UtcNow.Ticks}".ToLowerInvariant();
            containerName = tableNamePrefix;

            var account = CloudStorageAccount.Parse(connectionString);
            tableClient = account.CreateCloudTableClient();

            blobServiceClient = new BlobServiceClient(connectionString);
            var blobContainer = await blobServiceClient.CreateBlobContainerAsync(containerName);
            blobContainerClient = blobContainer.Value;
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

            await blobServiceClient.DeleteBlobContainerAsync(containerName);
        }

        [Test]
        public async Task TryLoadOngoingMigration_Should_Be_Null_When_No_Migration_Running()
        {
            // Arrange
            var timeoutsSource = new AspTimeoutsSource(connectionString, 10, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);

            // Act
            var currentMigration = await timeoutsSource.TryLoadOngoingMigration();

            // Assert
            Assert.IsNull(currentMigration);
        }

        [Test]
        public async Task TryLoadOngoingMigration_Should_return_tool_state_when_Migration_Running()
        {
            // Arrange
            var endpointName = nameof(TryLoadOngoingMigration_Should_return_tool_state_when_Migration_Running);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 10, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            await timeoutsSource.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            var currentMigration = await timeoutsSource.TryLoadOngoingMigration();

            // Assert
            Assert.IsNotNull(currentMigration);

            Assert.AreEqual(endpointName, currentMigration.EndpointName);
            Assert.AreEqual(runParameters, currentMigration.RunParameters);
            Assert.AreEqual(0, currentMigration.NumberOfBatches);
            Assert.AreEqual(MigrationStatus.StoragePrepared, currentMigration.Status);
        }

        [Test]
        public async Task TryLoadOngoingMigration_Should_return_tool_state_when_migration_failed_while_preparing()
        {
            // Arrange
            var endpointName = nameof(TryLoadOngoingMigration_Should_return_tool_state_when_migration_failed_while_preparing);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 10, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };

            var toolStateTable = tableClient.GetTableReference($"{tableNamePrefix}{AspConstants.ToolStateTableName}");
            await toolStateTable.CreateIfNotExistsAsync();

            var toolStateEntity = new ToolStateEntity { Status = MigrationStatus.Preparing, RunParameters = runParameters, EndpointName = endpointName, PartitionKey = ToolStateEntity.FixedPartitionKey, RowKey = "bar" };

            await toolStateTable.ExecuteAsync(TableOperation.Insert(toolStateEntity));

            // Act
            var currentMigration = await timeoutsSource.TryLoadOngoingMigration();

            // Assert
            Assert.IsNotNull(currentMigration);

            Assert.AreEqual(endpointName, currentMigration.EndpointName);
            Assert.AreEqual(runParameters, currentMigration.RunParameters);
            Assert.AreEqual(0, currentMigration.NumberOfBatches);
            Assert.AreEqual(MigrationStatus.Preparing, currentMigration.Status);
        }

        [Test]
        public async Task CheckMigrationInProgress_Should_be_false_when_no_migration_running()
        {
            // Arrange
            var timeoutsSource = new AspTimeoutsSource(connectionString, 10, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);

            // Act
            var currentMigration = await timeoutsSource.CheckIfAMigrationIsInProgress();

            // Assert
            Assert.IsFalse(currentMigration);
        }

        [Test]
        public async Task CheckMigrationInProgress_Should_be_true_when_migration_failed_during_prepare()
        {
            // Arrange
            var endpointName = nameof(CheckMigrationInProgress_Should_be_true_when_migration_failed_during_prepare);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 10, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var toolStateTable = tableClient.GetTableReference($"{tableNamePrefix}{AspConstants.ToolStateTableName}");
            await toolStateTable.CreateIfNotExistsAsync();

            var toolStateEntity = new ToolStateEntity { Status = MigrationStatus.Preparing, EndpointName = endpointName, PartitionKey = ToolStateEntity.FixedPartitionKey, RowKey = "bar" };

            await toolStateTable.ExecuteAsync(TableOperation.Insert(toolStateEntity));

            // Act
            var currentMigration = await timeoutsSource.CheckIfAMigrationIsInProgress();

            // Assert
            Assert.IsTrue(currentMigration);
        }

        [Test]
        public async Task CheckMigrationInProgress_Should_be_true_when_migration_running()
        {
            // Arrange
            var endpointName = nameof(CheckMigrationInProgress_Should_be_true_when_migration_running);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 10, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var toolStateTable = tableClient.GetTableReference($"{tableNamePrefix}{AspConstants.ToolStateTableName}");
            await toolStateTable.CreateIfNotExistsAsync();

            var toolStateEntity = new ToolStateEntity { Status = MigrationStatus.StoragePrepared, EndpointName = endpointName, PartitionKey = ToolStateEntity.FixedPartitionKey, RowKey = "bar" };

            await toolStateTable.ExecuteAsync(TableOperation.Insert(toolStateEntity));

            // Act
            var currentMigration = await timeoutsSource.CheckIfAMigrationIsInProgress();

            // Assert
            Assert.IsTrue(currentMigration);
        }

        [Test]
        public async Task Preparing_Creates_A_MigrationsEntity_And_Returns_It()
        {
            // Arrange
            var endpointName = nameof(Preparing_Creates_A_MigrationsEntity_And_Returns_It);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 10, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            // Act
            var currentMigration = await timeoutsSource.Prepare(cutOffDate, endpointName, runParameters);

            // Assert
            Assert.IsNotNull(currentMigration);

            Assert.AreEqual(endpointName, currentMigration.EndpointName);
            Assert.AreEqual(runParameters, currentMigration.RunParameters);
            Assert.AreEqual(0, currentMigration.NumberOfBatches);
            Assert.AreEqual(MigrationStatus.StoragePrepared, currentMigration.Status);
        }

        [Test]
        public async Task Preparing_Sets_The_Number_Of_Batches_Correctly()
        {
            // Arrange
            var endpointName = nameof(Preparing_Sets_The_Number_Of_Batches_Correctly);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 2, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            for (var x = 0; x < 3; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));
                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = endpointName,
                    Destination = "SomeDestination",
                    SagaId = Guid.NewGuid(),
                    StateAddress = x.ToString(),
                    Time = dateTime,
                    Headers = "Headers",
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
            }

            // Act
            var currentMigration = await timeoutsSource.Prepare(cutOffDate, endpointName, runParameters);

            // Assert
            Assert.IsNotNull(currentMigration);

            Assert.AreEqual(endpointName, currentMigration.EndpointName);
            Assert.AreEqual(runParameters, currentMigration.RunParameters);
            Assert.AreEqual(2, currentMigration.NumberOfBatches);
            Assert.AreEqual(MigrationStatus.StoragePrepared, currentMigration.Status);
        }

        [Test]
        public async Task Preparing_with_large_entities_Sets_The_Number_Of_Batches_Correctly()
        {
            // Arrange
            var endpointName = nameof(Preparing_with_large_entities_Sets_The_Number_Of_Batches_Correctly);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 1024, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            // the entity will roughly be 98 KB and we will store 50 of those which makes the actual payload be around 5 MB
            string destination = new string('a', 32 * 1024);
            string stateAddress = new string('s', 32 * 1024);
            string headers = new string('h', 32 * 1024);

            var batch = new TableBatchOperation();
            for (var x = 0; x < 50; x++)
            {
                var dateTime = cutOffDate.AddDays(2);
                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = endpointName,
                    Destination = destination,
                    SagaId = Guid.NewGuid(),
                    StateAddress = stateAddress,
                    Time = dateTime,
                    Headers = headers,
                };

                batch.Add(TableOperation.Insert(entity));

                if (batch.Count % 25 == 0)
                {
                    await endpointTimeoutTableName.ExecuteBatchAsync(batch);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await endpointTimeoutTableName.ExecuteBatchAsync(batch);
            }

            // Act
            var currentMigration = await timeoutsSource.Prepare(cutOffDate, endpointName, runParameters);

            // Assert
            Assert.IsNotNull(currentMigration);

            Assert.AreEqual(endpointName, currentMigration.EndpointName);
            Assert.AreEqual(runParameters, currentMigration.RunParameters);
            Assert.AreEqual(1, currentMigration.NumberOfBatches);
            Assert.AreEqual(MigrationStatus.StoragePrepared, currentMigration.Status);
        }

        [Test]
        public async Task Can_Read_Batch_By_Batch_Number()
        {
            // Arrange
            var endpointName = nameof(Can_Read_Batch_By_Batch_Number);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 1, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;
            var expectedDestinations = new List<string>();

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            for (var x = 0; x < 3; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));
                var stateAddress = x.ToString();
                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = endpointName,
                    Destination = endpointName + stateAddress,
                    SagaId = Guid.NewGuid(),
                    StateAddress = stateAddress,
                    Time = dateTime,
                    Headers = JsonConvert.SerializeObject(runParameters),
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
                await blobContainerClient.GetBlobClient(stateAddress)
                    .UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes("Hello World!")));

                expectedDestinations.Add(endpointName + stateAddress);
            }

            await timeoutsSource.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            for (var x = 0; x < 3; x++)
            {
                var batch = await timeoutsSource.ReadBatch(x + 1);
                expectedDestinations.Remove(batch.First().Destination);
            }

            // Assert
            // If all the batches were loaded correctly, the destinations would have been removed from the list.
            Assert.IsEmpty(expectedDestinations);
        }

        [Test]
        [TestCase(BatchState.Completed)]
        [TestCase(BatchState.Staged)]
        public async Task Marking_A_Batch_As_Complete_Updates_The_Status_Correctly(BatchState batchState)
        {
            // Arrange
            var endpointName = nameof(Marking_A_Batch_As_Complete_Updates_The_Status_Correctly);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 1, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            for (var x = 0; x < 3; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));
                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = endpointName,
                    Destination = endpointName,
                    SagaId = Guid.NewGuid(),
                    StateAddress = x.ToString(),
                    Time = dateTime,
                    Headers = "Headers",
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
            }

            await timeoutsSource.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            switch (batchState)
            {
                case BatchState.Staged:
                    await timeoutsSource.MarkBatchAsStaged(1);
                    break;
                case BatchState.Completed:
                    await timeoutsSource.MarkBatchAsCompleted(1);
                    break;
                case BatchState.Pending:
                    break;
                default:
                    return;
            }

            // Assert
            var query = new TableQuery<MigratedTimeoutDataEntity>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, 1.ToString(CultureInfo.InvariantCulture)))
                .Take(1);

            var migrationTableName = tableClient.GetTableReference($"{tableNamePrefix}{AspConstants.MigrationTableName}");

            var timeoutDataEntities = await migrationTableName.ExecuteQuerySegmentedAsync(query, null);

            Assert.True(timeoutDataEntities.All(t => t.BatchState == batchState), $"Expected all TimeoutEntity rows to have the batch state set to {batchState}");
        }

        [Test]
        public async Task Complete_Sets_The_MigrationStatus_Correctly()
        {
            // Arrange
            var endpointName = nameof(Marking_A_Batch_As_Complete_Updates_The_Status_Correctly);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 1, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            for (var x = 0; x < 3; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));

                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = endpointName,
                    Destination = endpointName,
                    SagaId = Guid.NewGuid(),
                    StateAddress = x.ToString(),
                    Time = dateTime,
                    Headers = "Headers",
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
            }

            var currentMigration = await timeoutsSource.Prepare(cutOffDate, endpointName, runParameters);

            for (var x = 0; x < currentMigration.NumberOfBatches; x++)
            {
                await timeoutsSource.MarkBatchAsCompleted(x + 1);
            }

            // Act
            await timeoutsSource.Complete();

            // Assert
            var loadedMigrationAfterCompletion = await timeoutsSource.TryLoadOngoingMigration();
            Assert.IsNull(loadedMigrationAfterCompletion);
        }

        [Test]
        public async Task Aborting_Returns_StagedTimeouts_Back_To_TimeoutEntity_Table()
        {
            // Arrange
            var endpointName = nameof(Aborting_Returns_StagedTimeouts_Back_To_TimeoutEntity_Table);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 1, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            for (var x = 0; x < 3; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));

                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = endpointName,
                    Destination = endpointName,
                    SagaId = Guid.NewGuid(),
                    StateAddress = x.ToString(),
                    Time = dateTime,
                    Headers = "Headers",
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
            }

            await timeoutsSource.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            await timeoutsSource.Abort();

            // Assert
            var query = new TableQuery<TimeoutDataEntity>()
                .Where(TableQuery.GenerateFilterCondition("OwningTimeoutManager", QueryComparisons.Equal, endpointName));
            var timeouts = await endpointTimeoutTableName.ExecuteQuerySegmentedAsync(query, null);
            Assert.AreEqual(3, timeouts.Results.Count);

            var currentAfterAborting = await timeoutsSource.TryLoadOngoingMigration();
            Assert.IsNull(currentAfterAborting);
        }

        [Test]
        public async Task Aborting_Unhides_The_TimeoutEntities()
        {
            // Arrange
            var endpointName = nameof(Aborting_Unhides_The_TimeoutEntities);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 1, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            var toolStateTable = tableClient.GetTableReference($"{tableNamePrefix}{AspConstants.ToolStateTableName}");
            await toolStateTable.CreateIfNotExistsAsync();

            var uniqueHiddenEndpointName = string.Format(AspConstants.MigrationHiddenEndpointNameFormat, "gurlugurlu", endpointName);
            var toolStateEntity = new ToolStateEntity { Status = MigrationStatus.StoragePrepared, EndpointName = endpointName, PartitionKey = ToolStateEntity.FixedPartitionKey, RowKey = "bar", UniqueHiddenEndpointName = uniqueHiddenEndpointName };

            await toolStateTable.ExecuteAsync(TableOperation.Insert(toolStateEntity));

            for (var x = 0; x < 3; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));

                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = uniqueHiddenEndpointName,
                    Destination = endpointName,
                    SagaId = Guid.NewGuid(),
                    StateAddress = x.ToString(),
                    Time = dateTime,
                    Headers = "Headers",
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
            }

            // Act
            await timeoutsSource.Abort();

            // Assert
            var query = new TableQuery<TimeoutDataEntity>()
                .Where(TableQuery.GenerateFilterCondition("OwningTimeoutManager", QueryComparisons.Equal, endpointName));
            var timeouts = await endpointTimeoutTableName.ExecuteQuerySegmentedAsync(query, null);
            Assert.AreEqual(3, timeouts.Results.Count);

            var currentAfterAborting = await timeoutsSource.TryLoadOngoingMigration();
            Assert.IsNull(currentAfterAborting);

            Assert.IsFalse(await timeoutsSource.CheckIfAMigrationIsInProgress());
        }

        [Test]
        public async Task Aborting_Unhides_The_TimeoutEntities_even_when_preparing_failed()
        {
            // Arrange
            var endpointName = nameof(Aborting_Unhides_The_TimeoutEntities);
            var timeoutsSource = new AspTimeoutsSource(connectionString, 1, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            var toolStateTable = tableClient.GetTableReference($"{tableNamePrefix}{AspConstants.ToolStateTableName}");
            await toolStateTable.CreateIfNotExistsAsync();

            var uniqueHiddenEndpointName = string.Format(AspConstants.MigrationHiddenEndpointNameFormat, "gurlugurlu", endpointName);
            var toolStateEntity = new ToolStateEntity() { Status = MigrationStatus.Preparing, EndpointName = endpointName, PartitionKey = ToolStateEntity.FixedPartitionKey, RowKey = "bar", UniqueHiddenEndpointName = uniqueHiddenEndpointName };

            await toolStateTable.ExecuteAsync(TableOperation.Insert(toolStateEntity));

            for (var x = 0; x < 3; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));

                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = uniqueHiddenEndpointName,
                    Destination = endpointName,
                    SagaId = Guid.NewGuid(),
                    StateAddress = x.ToString(),
                    Time = dateTime,
                    Headers = "Headers",
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
            }

            // Act
            await timeoutsSource.Abort();

            // Assert
            var query = new TableQuery<TimeoutDataEntity>()
                .Where(TableQuery.GenerateFilterCondition("OwningTimeoutManager", QueryComparisons.Equal, endpointName));
            var timeouts = await endpointTimeoutTableName.ExecuteQuerySegmentedAsync(query, null);
            Assert.AreEqual(3, timeouts.Results.Count);

            var currentAfterAborting = await timeoutsSource.TryLoadOngoingMigration();
            Assert.IsNull(currentAfterAborting);

            Assert.IsFalse(await timeoutsSource.CheckIfAMigrationIsInProgress());
        }

        [Test]
        public async Task ListEndpoint_should_ignore_endpoints_outside_cutoff_date()
        {
            // Arrange
            var timeoutsSource = new AspTimeoutsSource(connectionString, 10, containerName, fakeEndpointName, fakeEndpointTimeoutTableName, tablePrefix: tableNamePrefix);
            var cutOffDate = DateTime.UtcNow;

            var endpointTimeoutTableName = tableClient.GetTableReference($"{tableNamePrefix}{fakeEndpointTimeoutTableName}");
            await endpointTimeoutTableName.CreateIfNotExistsAsync();

            for (var x = 0; x < 5; x++)
            {
                int value = x + 1;
                var dateTime = cutOffDate.AddDays(value);

                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = fakeEndpointName,
                    Destination = $"Destination{value}",
                    SagaId = Guid.NewGuid(),
                    StateAddress = value.ToString(),
                    Time = dateTime,
                    Headers = "Headers",
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
            }

            for (var x = 0; x < 5; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));

                var entity = new TimeoutDataEntity(dateTime.ToString(AspConstants.PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = "DifferentEndpoint",
                    Destination = $"Destination{x}",
                    SagaId = Guid.NewGuid(),
                    StateAddress = x.ToString(),
                    Time = dateTime,
                    Headers = "Headers",
                };

                await endpointTimeoutTableName.ExecuteAsync(TableOperation.Insert(entity));
            }

            var result = await timeoutsSource.ListEndpoints(cutOffDate);

            Assert.AreEqual(1, result.Count);

            var singleElement = result[0];

            Assert.AreEqual(5, singleElement.NrOfTimeouts);
            Assert.AreEqual(DateTime.UtcNow.AddDays(1).Day, singleElement.ShortestTimeout.Day);
            Assert.AreEqual(DateTime.UtcNow.AddDays(5).Day, singleElement.LongestTimeout.Day);
            CollectionAssert.AreEquivalent(new List<string>
            {
                "Destination1",
                "Destination2",
                "Destination3",
                "Destination4",
                "Destination5",
            }, singleElement.Destinations);
            Assert.AreEqual(fakeEndpointName, singleElement.EndpointName);
        }
    }
}
