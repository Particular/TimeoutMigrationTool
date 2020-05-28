using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.RavenDB;

namespace TimeoutMigrationTool.Raven3.Tests
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
        public async Task WhenGettingTimeoutStateAndNoneIsFoundNullIsReturned()
        {
            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var toolState = await timeoutStorage.GetToolState();

            Assert.That(toolState, Is.Null);
        }

        [Test]
        public async Task WhenGettingTimeoutStateAndOneIsFoundWeReturnIt()
        {
            await SaveToolState(SetupToolState(DateTime.Now.AddDays(-1)));

            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var retrievedToolState = await timeoutStorage.GetToolState();

            Assert.That(retrievedToolState, Is.Not.Null);
            Assert.That(retrievedToolState.Status, Is.EqualTo(MigrationStatus.NeverRun));
            Assert.IsEmpty(retrievedToolState.Batches);
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatches()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var batches = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), endpoint);

            Assert.That(batches.Count, Is.EqualTo(2));
            Assert.That(batches.First().TimeoutIds.Length, Is.EqualTo(RavenConstants.DefaultPagingSize));
            Assert.That(batches.Skip(1).First().TimeoutIds.Length,
                Is.EqualTo(nrOfTimeouts - RavenConstants.DefaultPagingSize));
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatchesWhenMoreEndpointsAreAvailable()
        {
            endpoint.EndpointName = "B";
            await InitTimeouts(nrOfTimeouts, true);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var batches = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), endpoint);

            Assert.That(batches.Count, Is.EqualTo(1));
            Assert.That(batches.First().TimeoutIds.Length, Is.EqualTo(500));
        }

        [Test]
        public async Task WhenVerifyingPrepareAndFoundExistingBatchInfosReturnsFalse()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            var toolState = SetupToolState(cutOffTime);
            await SaveToolState(toolState);

            var storage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var batches = await storage.Prepare(cutOffTime, endpoint);
            toolState.InitBatches(batches);
            await SaveToolState(toolState);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var canPrepareStorage = await timeoutStorage.CanPrepareStorage();
            Assert.That(canPrepareStorage, Is.False);
        }

        [Test]
        public async Task WhenVerifyingPrepareAndSystemIsCleanInfosReturnsTrue()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var canPrepareStorage = await timeoutStorage.CanPrepareStorage();
            Assert.That(canPrepareStorage, Is.True);
        }

        [Test]
        public async Task WhenRemovingTheToolStateStoreIsCleanedUp()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await timeoutStorage.RemoveToolState();

            using (var httpClient = new HttpClient())
            {
                var getStateUrl = $"{ServerName}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";
                var result = await httpClient.GetAsync(getStateUrl);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
            }
        }

        [Test]
        public async Task WhenStoringTheToolStateTheToolStateIsUpdated()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            toolState.Status = MigrationStatus.StoragePrepared;
            await timeoutStorage.StoreToolState(toolState);

            var updatedToolState = await GetToolState();
            Assert.That(updatedToolState.Status, Is.EqualTo(MigrationStatus.StoragePrepared));
        }
    }
}