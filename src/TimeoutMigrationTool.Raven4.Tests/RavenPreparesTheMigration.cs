using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.RavenDB;

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
            var toolState = SetupToolState(DateTime.Now.AddDays(-1));
            await SaveToolState(toolState);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var retrievedToolState = await timeoutStorage.GetOrCreateToolState();

            Assert.That(retrievedToolState.IsStoragePrepared, Is.False);
            Assert.IsEmpty(retrievedToolState.Batches);
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatches()
        {
            var toolState = SetupToolState(DateTime.Now.AddDays(-1));

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await timeoutStorage.Prepare(toolState);

            Assert.That(batches.Count, Is.EqualTo(2));
            Assert.That(batches.First().TimeoutIds.Length, Is.EqualTo(RavenConstants.DefaultPagingSize));
            Assert.That(batches.Skip(1).First().TimeoutIds.Length,
                Is.EqualTo(nrOfTimeouts - RavenConstants.DefaultPagingSize));
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedButWeFindBatchInfoWeClearItAndStartOver()
        {
            var toolState = SetupToolState(DateTime.Now.AddDays(-1));
            var item = await GetTimeout("TimeoutDatas/1").ConfigureAwait(false);
            var originalTimeoutManager = item.OwningTimeoutManager;

            await SetupExistingBatchInfoInDatabase();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.CleanupAnyExistingBatchesIfNeeded(new ToolState
            {
                IsStoragePrepared = false
            });

            var ravenDbReader = new RavenDbReader<BatchInfo>(ServerName, databaseName, RavenDbVersion.Four);
            var savedBatches = await ravenDbReader.GetItems(x => true, "batch", CancellationToken.None);
            var modifiedItem = await GetTimeout("TimeoutDatas/1").ConfigureAwait(false);
            Assert.That(savedBatches.Count, Is.EqualTo(0));
            Assert.That(modifiedItem.OwningTimeoutManager, Is.EqualTo(originalTimeoutManager));
        }

        [Test]
        public async Task WhenTheStorageHasBeenPreparedWeReturnStoredBatches()
        {
            var toolState = SetupToolState(DateTime.Now.AddDays(-1), true);
            await SaveToolState(toolState);
            await SetupExistingBatchInfoInDatabase();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await sut.Prepare(toolState);

            Assert.That(batches.Count, Is.EqualTo(2));
        }

        private ToolState SetupToolState(DateTime cutoffTime, bool isStoragePrepared = false)
        {
            var toolState = new ToolState
            {
                IsStoragePrepared = isStoragePrepared,
                Parameters = new Dictionary<string, string>
                {
                    {ApplicationOptions.CutoffTime, cutoffTime.ToString()},
                    {ApplicationOptions.RavenServerUrl, ServerName},
                    {ApplicationOptions.RavenDatabaseName, databaseName},
                    {ApplicationOptions.RavenVersion, RavenDbVersion.Four.ToString()},
                }
            };
            return toolState;
        }

        private async Task SetupExistingBatchInfoInDatabase()
        {
            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now);
        }

        private async Task SaveToolState(ToolState toolState)
        {
            using (var httpClient = new HttpClient())
            {
                var insertStateUrl = $"{ServerName}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";

                var serializeObject = JsonConvert.SerializeObject(toolState);
                var httpContent = new StringContent(serializeObject);

                var result = await httpClient.PutAsync(insertStateUrl, httpContent);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
            }
        }

    }
}