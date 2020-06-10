namespace TimeoutMigrationTool.Raven4.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    public class RavenAbortsTheMigration : RavenTimeoutStorageTestSuite
    {
        private readonly int nrOfTimeouts = 1500;

        [Test]
        public async Task WhenThereIsStateAbortShouldDeleteState()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.Abort();

            var storedSate = await GetToolState();
            Assert.That(storedSate, Is.Null);
        }

        [Test]
        public async Task WhenAbortingOnPreparedStorageStateShouldBeCleanedUp()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            var storage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await storage.Prepare(cutOffTime, endpoint, new Dictionary<string, string>());

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
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

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.CleanupExistingBatchesAndResetTimeouts(preparedBatches, incompleteBatches);

            var ravenDbReader = new Raven4Adapter(ServerName, databaseName);
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

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.CleanupExistingBatchesAndResetTimeouts(preparedBatches, incompleteBatches);

            var ravenDbReader = new Raven4Adapter(ServerName, databaseName);
            var completeBatchFromStorage = await ravenDbReader.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{completeBatch.Number}", (doc, id) => { });
            var resetTimeouts = await ravenDbReader.GetDocuments<TimeoutData>(x => completeBatch.TimeoutIds.Contains(x.Id), "TimeoutDatas", (doc, id) => doc.Id = id);

            Assert.That(completeBatchFromStorage, Is.Null);
            Assert.That(resetTimeouts.Select(t => t.OwningTimeoutManager), Is.All.Matches<string>(x => x.StartsWith(RavenConstants.MigrationOngoingPrefix)));
        }
    }
}