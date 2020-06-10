namespace TimeoutMigrationTool.Raven4.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    public class RavenPreparesTheMigration : RavenTimeoutStorageTestSuite
    {
        [SetUp]
        public async Task Setup()
        {
            await InitTimeouts(nrOfTimeouts);
        }

        [Test]
        public async Task WhenGettingTimeoutStateAndNoneIsFoundNullIsReturned()
        {
            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var toolState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.That(toolState, Is.Null);
        }

        [Test]
        public async Task WhenGettingTimeoutStateAndOneIsFoundWeReturnIt()
        {
            await SaveToolState(SetupToolState(DateTime.Now.AddDays(-1)));

            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var retrievedToolState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.That(retrievedToolState, Is.Not.Null);
            Assert.That(retrievedToolState.Status, Is.EqualTo(MigrationStatus.StoragePrepared));
            Assert.IsEmpty(retrievedToolState.Batches);
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatches()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), endpoint, new Dictionary<string, string>());

            Assert.That(toolState.Batches.Count, Is.EqualTo(2));
            Assert.That(toolState.Batches.First().TimeoutIds.Length, Is.EqualTo(RavenConstants.DefaultPagingSize));
            Assert.That(toolState.Batches.Skip(1).First().TimeoutIds.Length,
                Is.EqualTo(nrOfTimeouts - RavenConstants.DefaultPagingSize));
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatchesWhenMoreEndpointsAreAvailable()
        {
            endpoint.EndpointName = "B";
            await InitTimeouts(nrOfTimeouts, true);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), endpoint, new Dictionary<string, string>());

            Assert.That(toolState.Batches.Count, Is.EqualTo(1));
            Assert.That(toolState.Batches.First().TimeoutIds.Length, Is.EqualTo(500));
        }

        [Test]
        public async Task WhenRemovingTheToolStateStoreIsCleanedUp()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await timeoutStorage.RemoveToolState();


            var getStateUrl = $"{ServerName}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";
            var result = await httpClient.GetAsync(getStateUrl);
            Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
        }

        [Test]
        public async Task WhenStoringTheToolStateTheToolStateIsUpdated()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);

            var updatedToolState = await timeoutStorage.TryLoadOngoingMigration();
            Assert.That(updatedToolState.Status, Is.EqualTo(MigrationStatus.StoragePrepared));
        }

        private readonly int nrOfTimeouts = 1500;
    }
}