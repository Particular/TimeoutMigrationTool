namespace TimeoutMigrationTool.ASQ.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Table;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.ASQ;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.AzureStorageConnectionString)]
    class ASQTargetTests
    {
        string connectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.AzureStorageConnectionString);
        CloudTableClient tableClient;
        string endpointName;

        [SetUp]
        public void Setup()
        {
            endpointName = $"TestEndpoint{Guid.NewGuid().ToString().Replace("-", string.Empty)}";

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            tableClient = cloudStorageAccount.CreateCloudTableClient();
        }

        [TearDown]
        public async Task TearDown()
        {
            var nameProvider = new DelayedDeliveryTableNameProvider();

            await DeleteTable(nameProvider.GetStagingTableName(endpointName));
            await DeleteTable(nameProvider.GetDelayedDeliveryTableName(endpointName));
        }

        [Test]
        public async Task AbleToMigrate_fails_if_Timeout_table_Does_not_exist()
        {
            // Arrange
            var timeoutTarget = new ASQTarget(connectionString, new DelayedDeliveryTableNameProvider("TimeoutTableThatDoesNotExist"));

            // Act
            var ableToMigrate = await timeoutTarget.AbleToMigrate(new EndpointInfo { EndpointName = endpointName });

            Assert.Multiple(() =>
            {
                // Assert
                Assert.That(ableToMigrate.CanMigrate, Is.False);
                Assert.That(ableToMigrate.Problems[0], Is.EqualTo("Target delayed delivery table TimeoutTableThatDoesNotExist does not exist."));
            });
        }

        [Test]
        public async Task AbleToMigrate_fails_with_incorrect_connection_string()
        {
            // Arrange
            string fakeConnectionString = "DefaultEndpointsProtocol=https;AccountName=fakename;AccountKey=g94OvNO9o3sVan5eipemQEHmU8zD2M9iq98E8nKSdR2bTuQB1hi07Yd1/8dDw6+1jGI2klWjpvoDahHPhR/3og==";
            var account = CloudStorageAccount.Parse(fakeConnectionString);
            var client = account.CreateCloudTableClient();
            client.DefaultRequestOptions.MaximumExecutionTime = TimeSpan.FromSeconds(2);

            var timeoutTarget = new ASQTarget(client, new DelayedDeliveryTableNameProvider("TimeoutTableThatDoesNotExist"));

            // Act
            var ableToMigrate = await timeoutTarget.AbleToMigrate(new EndpointInfo { EndpointName = endpointName });

            Assert.Multiple(() =>
            {
                // Assert
                Assert.That(ableToMigrate.CanMigrate, Is.False);
                Assert.That(ableToMigrate.Problems[0], Does.StartWith("Unable to connect to the storage instance on account 'fakename'. Verify the connection string. Exception message '"));
            });
        }

        [Test]
        public async Task AbleToMigrate_passes_if_Timeout_table_exists()
        {
            // Arrange
            await CreateTimeoutTable($"T{endpointName}");

            var timeoutTarget = new ASQTarget(connectionString, new DelayedDeliveryTableNameProvider($"T{endpointName}"));

            // Act
            var ableToMigrate = await timeoutTarget.AbleToMigrate(new EndpointInfo { EndpointName = endpointName });

            // Assert
            Assert.That(ableToMigrate.CanMigrate, Is.True);
        }

        [Test]
        public async Task Prepare_creates_the_staging_queue()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();
            var timeoutTarget = new ASQTarget(connectionString, nameProvider);

            // Act
            await using var migrator = await timeoutTarget.PrepareTargetEndpointBatchMigrator(endpointName);

            // Assert
            Assert.That(await DoesTableExist(nameProvider.GetStagingTableName(endpointName)).ConfigureAwait(true), Is.True);
        }

        [Test]
        public async Task StageBatch_inserts_timeouts_into_the_staging_queue()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();

            var timeoutTarget = new ASQTarget(connectionString, nameProvider);
            await using var migrator = await timeoutTarget.PrepareTargetEndpointBatchMigrator(endpointName);

            // Act
            var numberStaged = await migrator.StageBatch(
            [
                new TimeoutData
                {
                    Id = "SomeID",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeMessageId" }
                    },
                    Destination = "SomeDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                },
                new TimeoutData
                {
                    Id = "SomeOtherId",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeOtherMessageId" }
                    },
                    Destination = "SomeOtherDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 13, 13, DateTimeKind.Utc)
                },
            ], 1);

            // Assert
            var recordsInTable = await ReadTimeoutsFromTable(nameProvider.GetStagingTableName(endpointName));

            Assert.Multiple(() =>
            {
                Assert.That(recordsInTable, Has.Count.EqualTo(2));
                Assert.That(numberStaged, Is.EqualTo(2));
            });
        }

        [Test]
        public async Task CompleteBatch_Moves_All_Entries_From_Staging_To_DelayedMessageTable()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();

            await CreateTimeoutTable(nameProvider.GetDelayedDeliveryTableName(endpointName));

            var timeoutTarget = new ASQTarget(connectionString, nameProvider);
            await using var migrator = await timeoutTarget.PrepareTargetEndpointBatchMigrator(endpointName);

            await migrator.StageBatch(
            [
                new TimeoutData
                {
                    Id = "SomeID",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeMessageId" }
                    },
                    Destination = "SomeDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                },
                new TimeoutData
                {
                    Id = "SomeOtherId",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeOtherMessageId" }
                    },
                    Destination = "SomeOtherDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 13, 13, DateTimeKind.Utc)
                },
            ], 1);

            // Act
            var numberCompleted = await migrator.CompleteBatch(1);

            // Assert
            var recordsInTimeoutTable = await ReadTimeoutsFromTable(nameProvider.GetDelayedDeliveryTableName(endpointName));

            Assert.Multiple(() =>
            {
                Assert.That(recordsInTimeoutTable, Has.Count.EqualTo(2));
                Assert.That(numberCompleted, Is.EqualTo(2));
            });
        }

        [Test]
        public async Task CompleteBatch_Removes_All_Entries_From_Staging()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();

            await CreateTimeoutTable(nameProvider.GetDelayedDeliveryTableName(endpointName));

            var timeoutTarget = new ASQTarget(connectionString, nameProvider);
            await using var migrator = await timeoutTarget.PrepareTargetEndpointBatchMigrator(endpointName);

            await migrator.StageBatch(
            [
                new TimeoutData
                {
                    Id = "SomeID",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeMessageId" }
                    },
                    Destination = "SomeDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                },
                new TimeoutData
                {
                    Id = "SomeOtherId",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeOtherMessageId" }
                    },
                    Destination = "SomeOtherDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 13, 13, DateTimeKind.Utc)
                },
            ], 1);

            // Act
            await migrator.CompleteBatch(1);

            // Assert
            var recordsInStagingTable = await ReadTimeoutsFromTable(nameProvider.GetStagingTableName(endpointName));

            Assert.That(recordsInStagingTable.Count, Is.EqualTo(0));
        }

        [Test]
        public async Task Abort_Removes_Staging_Table()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();

            await CreateTimeoutTable(nameProvider.GetDelayedDeliveryTableName(endpointName));

            var timeoutTarget = new ASQTarget(connectionString, nameProvider);
            await using var migrator = await timeoutTarget.PrepareTargetEndpointBatchMigrator(endpointName);

            // Act
            await timeoutTarget.Abort(endpointName);

            // Assert
            var stagingTableExists = await DoesTableExist(nameProvider.GetStagingTableName(endpointName));

            Assert.That(stagingTableExists, Is.False);
        }

        [Test]
        public async Task Complete_Throws_If_Messages_Are_Still_Staged()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();

            await CreateTimeoutTable(nameProvider.GetDelayedDeliveryTableName(endpointName));

            var timeoutTarget = new ASQTarget(connectionString, nameProvider);
            await using var migrator = await timeoutTarget.PrepareTargetEndpointBatchMigrator(endpointName);

            var numberStaged = await migrator.StageBatch(
            [
                new TimeoutData
                {
                    Id = "SomeID",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeMessageId" }
                    },
                    Destination = "SomeDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                },
                new TimeoutData
                {
                    Id = "SomeOtherId",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeOtherMessageId" }
                    },
                    Destination = "SomeOtherDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 13, 13, DateTimeKind.Utc)
                },
            ], 1);

            // Assert
            Assert.ThrowsAsync<Exception>(async () =>
            {
                // Act
                await timeoutTarget.Complete(endpointName);
            });
        }

        [Test]
        public async Task Complete_Removes_Staging_Queue_If_Empty()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();

            await CreateTimeoutTable(nameProvider.GetDelayedDeliveryTableName(endpointName));

            var timeoutTarget = new ASQTarget(connectionString, nameProvider);
            await using var migrator = await timeoutTarget.PrepareTargetEndpointBatchMigrator(endpointName);

            await migrator.StageBatch(
            [
                new TimeoutData
                {
                    Id = "SomeID",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeMessageId" }
                    },
                    Destination = "SomeDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                },
                new TimeoutData
                {
                    Id = "SomeOtherId",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeOtherMessageId" }
                    },
                    Destination = "SomeOtherDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 13, 13, DateTimeKind.Utc)
                },
            ], 1);

            await migrator.CompleteBatch(1);

            // Act
            await timeoutTarget.Complete(endpointName);

            // Assert
            var stagingTableExists = await DoesTableExist(nameProvider.GetStagingTableName(endpointName));

            Assert.That(stagingTableExists, Is.False);
        }

        [Test]
        public async Task Staging_with_large_entities_batches_respecting_size_limitations()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();
            var endpointName = nameof(Staging_with_large_entities_batches_respecting_size_limitations);
            var cutOffDate = DateTime.UtcNow;
            var random = new Random();
            var target = new ASQTarget(connectionString, nameProvider);
            var timeouts = new List<TimeoutData>();

            // the entity will roughly be 98 KB and we will store 50 of those which makes the actual payload be around 5 MB
            for (var x = 0; x < 50; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));

                timeouts.Add(new TimeoutData
                {
                    Destination = new string('a', 32 * 1024),
                    Headers = new Dictionary<string, string> { { "a", new string('h', 16 * 1024) } },
                    Time = dateTime,
                    Id = Guid.NewGuid().ToString(),
                    State = new byte[32 * 1024],
                    SagaId = Guid.NewGuid(),
                    OwningTimeoutManager = this.endpointName
                });
            }

            // Act
            var result = await target.PrepareTargetEndpointBatchMigrator(endpointName);
            var stageResult = await result.StageBatch(timeouts, 1);

            // Assert
            Assert.That(stageResult, Is.EqualTo(timeouts.Count));
        }

        [Test]
        public async Task Completing_with_large_entities_batches_respecting_size_limitations()
        {
            // Arrange
            var nameProvider = new DelayedDeliveryTableNameProvider();
            var endpointName = nameof(Completing_with_large_entities_batches_respecting_size_limitations);
            var cutOffDate = DateTime.UtcNow;
            var random = new Random();
            var timeouts = new List<StagedDelayedMessageEntity>();

            var target = new ASQTarget(connectionString, nameProvider);
            var stagingTableName = nameProvider.GetStagingTableName(endpointName);
            await CreateTimeoutTable(stagingTableName);
            await CreateTimeoutTable(nameProvider.GetDelayedDeliveryTableName(endpointName));
            var stagingTable = tableClient.GetTableReference(stagingTableName);

            // the entity will roughly be 98 KB and we will store 50 of those which makes the actual payload be around 5 MB
            for (var x = 0; x < 50; x++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 5));
                var messageId = Guid.NewGuid().ToString();
                var entity = new StagedDelayedMessageEntity
                {
                    Destination = new string('a', 32 * 1024),
                    Headers = new string('a', 32 * 1024),
                    Time = dateTime,
                    MessageId = messageId,
                    Body = new byte[32 * 1024],
                    RowKey = $"{messageId}_{dateTime.ToString($"yyyyMMddHHmmss")}",
                    PartitionKey = "1"
                };

                await stagingTable.ExecuteAsync(TableOperation.Insert(entity));
                timeouts.Add(entity);
            }

            // Act
            var result = await target.PrepareTargetEndpointBatchMigrator(endpointName);
            var completeResult = await result.CompleteBatch(1);

            // Assert
            Assert.That(completeResult, Is.EqualTo(timeouts.Count));
        }

        async Task DeleteTable(string tableName)
        {
            var table = tableClient.GetTableReference(tableName);
            await table.DeleteIfExistsAsync();
        }

        async Task CreateTimeoutTable(string tableName)
        {
            var table = tableClient.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();
        }

        async Task<bool> DoesTableExist(string tableName)
        {
            var table = tableClient.GetTableReference(tableName);
            return await table.ExistsAsync();
        }

        async Task<IList<DelayedMessageEntity>> ReadTimeoutsFromTable(string tableName)
        {
            var table = tableClient.GetTableReference(tableName);

            var delayedMessages = await table.ExecuteQueryAsync(new TableQuery<DelayedMessageEntity>(), CancellationToken.None);

            return delayedMessages;
        }
    }
}