namespace TimeoutMigrationTool.Raven3.IntegrationTests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    public class RavenAbortsTheMigration : RavenTimeoutStorageTestSuite
    {
        private readonly int nrOfTimeouts = 1500;

        [Test]
        public void WhenThereIsNoStateAbortShouldNotFail()
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
            {
                var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
                await sut.Abort();
            });
        }

        [Test]
        public async Task WhenThereIsStateAndNoTimeoutsAbortShouldDeleteState()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await sut.Abort();

            var storedSate = await GetToolState();
            Assert.That(storedSate, Is.Null);
        }

        [Test]
        public async Task WhenAbortingOnPreparedStorageStateShouldBeCleanedUp()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            var toolState = SetupToolState(cutOffTime);
            await SaveToolState(toolState);
            await InitTimeouts(nrOfTimeouts);

            var storage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var batches = await storage.Prepare(cutOffTime, endpoint);
            toolState.InitBatches(batches);
            await SaveToolState(toolState);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await sut.Abort();

            var storedSate = await GetToolState();
            Assert.That(storedSate, Is.Null);
        }

        [Test]
        public async Task WhenCleaningUpBatchesThenTimeoutsInIncompleteBatchesAreReset()
        {
            await InitTimeouts(nrOfTimeouts);
            SetupToolState(DateTime.Now.AddDays(-1));
            var preparedBatches = await SetupExistingBatchInfoInDatabase();
            var incompleteBatches = preparedBatches.Skip(1).Take(1).ToList();
            var incompleteBatch = incompleteBatches.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await sut.CleanupExistingBatchesAndResetTimeouts(preparedBatches, incompleteBatches);

            var ravenDbReader = new Raven3Adapter(ServerName, databaseName);
            var incompleteBatchFromStorage = await ravenDbReader.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{incompleteBatch.Number}", (doc, id) => { });
            var resetTimeouts = await ravenDbReader.GetDocuments<TimeoutData>(x => incompleteBatch.TimeoutIds.Contains(x.Id), "TimeoutDatas", (doc, id) => doc.Id = id);

            Assert.That(incompleteBatchFromStorage, Is.Null);
            Assert.That(resetTimeouts.Select(t => t.OwningTimeoutManager), Is.All.Matches<string>(x => !x.StartsWith(RavenConstants.MigrationOngoingPrefix)));
        }

        [Test]
        public async Task WhenCleaningUpBatchesThenTimeoutsInCompleteBatchesAreNotReset()
        {
            await InitTimeouts(nrOfTimeouts);
            SetupToolState(DateTime.Now.AddDays(-1));
            var preparedBatches = await SetupExistingBatchInfoInDatabase();
            var incompleteBatches = preparedBatches.Skip(1).Take(1).ToList();
            var completeBatch = preparedBatches.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await sut.CleanupExistingBatchesAndResetTimeouts(preparedBatches, incompleteBatches);

            var ravenDbReader = new Raven3Adapter(ServerName, databaseName);
            var completeBatchFromStorage = await ravenDbReader.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{completeBatch.Number}", (doc, id) => { });
            var resetTimeouts = await ravenDbReader.GetDocuments<TimeoutData>(x => completeBatch.TimeoutIds.Contains(x.Id), "TimeoutDatas", (doc, id) => doc.Id = id);

            Assert.That(completeBatchFromStorage, Is.Null);
            Assert.That(resetTimeouts.Select(t => t.OwningTimeoutManager), Is.All.Matches<string>(x => x.StartsWith(RavenConstants.MigrationOngoingPrefix)));
        }
    }
}