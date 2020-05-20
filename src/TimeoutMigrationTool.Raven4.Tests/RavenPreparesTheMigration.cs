using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.RavenDB;
using Particular.TimeoutMigrationTool.RavenDB.HttpCommands;

namespace TimeoutMigrationTool.Raven4.Tests
{
    public class RavenPreparesTheMigration : RavenTimeoutStorageTestSuite
    {
        private readonly int nrOfTimeouts = 1500;

        [SetUp]
        public async Task Setup()
        {
            await InitTimeouts(nrOfTimeouts);
        }

        [Test]
        public async Task WhenGettingTimeoutStateAndNoneIsFoundWeCreateOne()
        {
            using (var httpClient = new HttpClient())
            {
                var getStateUrl = $"{ServerName}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";
                var result = await httpClient.GetAsync(getStateUrl);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
            }

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var toolState = await timeoutStorage.GetOrCreateToolState();

            Assert.That(toolState.IsStoragePrepared, Is.False);
            Assert.IsEmpty(toolState.Batches);
        }

        [Test]
        public async Task WhenGettingTimeoutStateAndOneIsFoundWeReturnIt()
        {
            await SetupExistingToolStateInDatabase(false);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var retrievedToolState = await timeoutStorage.GetOrCreateToolState();

            Assert.That(retrievedToolState.IsStoragePrepared, Is.False);
            Assert.IsEmpty(retrievedToolState.Batches);
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatches()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await timeoutStorage.Prepare();

            Assert.That(batches.Count, Is.EqualTo(2));
            Assert.That(batches.First().TimeoutIds.Length, Is.EqualTo(RavenConstants.DefaultPagingSize));
            Assert.That(batches.Skip(1).First().TimeoutIds.Length,
                Is.EqualTo(nrOfTimeouts - RavenConstants.DefaultPagingSize));
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedButWeFindBatchInfoWeClearItAndStartOver()
        {
            await SetupExistingBatchInfoInDatabase();

            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await timeoutStorage.Prepare();

            var ravenDbReader = new RavenDbReader<BatchInfo>(ServerName, databaseName, RavenDbVersion.Four);
            var savedBatches = await ravenDbReader.GetItems(x => true, "batch", CancellationToken.None);
            Assert.That(savedBatches.Count, Is.EqualTo(2));
        }

        [Test]
        public async Task WhenTheStorageHasBeenPreparedWeReturnStoredBatches()
        {
            await SetupExistingToolStateInDatabase(true);
            await SetupExistingBatchInfoInDatabase();

            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await timeoutStorage.Prepare();

            Assert.That(batches.Count, Is.EqualTo(1));
        }

        private async Task SetupExistingToolStateInDatabase(bool isStoragePrepared)
        {
            using (var httpClient = new HttpClient())
            {
                var insertStateUrl = $"{ServerName}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";

                // Insert the tool state data
                var toolState = new ToolState
                {
                    IsStoragePrepared = isStoragePrepared
                };

                var serializeObject = JsonConvert.SerializeObject(toolState);
                var httpContent = new StringContent(serializeObject);

                var result = await httpClient.PutAsync(insertStateUrl, httpContent);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
            }
        }

        private async Task SetupExistingBatchInfoInDatabase()
        {
            // setup some batchdata
            var batch = new BatchInfo
            {
                Number = 5,
                State = BatchState.Pending,
                TimeoutIds = new[]
                {
                    "TimeoutDatas/1", "TimeoutDatas/2"
                }
            };

            using (var httpClient = new HttpClient())
            {
                var bulkInsertUrl = $"{ServerName}/databases/{databaseName}/bulk_docs";
                var bulkCreateBatchAndUpdateTimeoutsCommand = new
                {
                    Commands = new[]
                    {
                        new PutCommand
                        {
                            Id = $"batch/{batch.Number}",
                            Type = "PUT",
                            ChangeVector = (object) null,
                            Document = batch
                        }
                    }
                };

                var serializedCommands = JsonConvert.SerializeObject(bulkCreateBatchAndUpdateTimeoutsCommand);
                var result = await httpClient.PostAsync(bulkInsertUrl,
                    new StringContent(serializedCommands, Encoding.UTF8, "application/json")).ConfigureAwait(false);
                result.EnsureSuccessStatusCode();
            }
        }
    }
}