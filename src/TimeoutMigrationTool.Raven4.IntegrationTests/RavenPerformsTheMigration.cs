namespace TimeoutMigrationTool.Raven4.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    public class RavenPerformsTheMigration : RavenTimeoutStorageTestSuite
    {
        private readonly int nrOfTimeouts = 2000;

        [SetUp]
        public async Task Setup()
        {
            await InitTimeouts(nrOfTimeouts);
        }

        [Test]
        public async Task WhenReadingABatchAllTimeoutsInBatchAreReturned()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now.AddDays(-1), endpoint);

            var batchToVerify = batches.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var timeoutDatasInBatch = await sut.ReadBatch(batchToVerify.Number);

            Assert.That(batchToVerify.TimeoutIds.Length, Is.EqualTo(timeoutDatasInBatch.Count));
        }

        [Test]
        public async Task WhenMarkingBatchAsStagedThenBatchStatusIsUpdated()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now.AddDays(-1), endpoint);

            var batchToVerify = batches.First();
            Assert.That(batchToVerify.State, Is.EqualTo(BatchState.Pending));

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.MarkBatchAsStaged(batchToVerify.Number);

            var reader = new Raven4Adapter(ServerName, databaseName);
            var updatedBatch = await reader.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{batchToVerify.Number}", (batch, id) => { });

            Assert.That(updatedBatch.State, Is.EqualTo(BatchState.Staged));
        }

        [Test]
        public async Task WhenCompletingABatchCurrentBatchShouldBeMovedUp()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), endpoint, new Dictionary<string, string>());

            var batchToVerify = toolState.Batches.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.MarkBatchAsCompleted(batchToVerify.Number);

            var reader = new Raven4Adapter(ServerName, databaseName);
            var updatedBatch = await reader.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{batchToVerify.Number}", (batch, id) => { });

            toolState = await sut.TryLoadOngoingMigration();
            var currentBatch = toolState.GetCurrentBatch();

            Assert.That(updatedBatch.State, Is.EqualTo(BatchState.Completed));
            Assert.That(currentBatch, Is.Not.Null);
            Assert.That(currentBatch.Number, Is.EqualTo(batchToVerify.Number + 1));
        }

        [Test]
        public async Task WhenCompletingABatchTimeoutsAreMarkedDone()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), endpoint, new Dictionary<string, string>());

            var batchToVerify = toolState.Batches.First();
            var timeoutIdToVerify = batchToVerify.TimeoutIds.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.MarkBatchAsCompleted(batchToVerify.Number);

            var reader = new Raven4Adapter(ServerName, databaseName);
            var updatedTimeout = await reader.GetDocument<TimeoutData>(timeoutIdToVerify,
                (timeoutData, id) => { timeoutData.Id = id; });

            Assert.That(updatedTimeout.OwningTimeoutManager.StartsWith(RavenConstants.MigrationDonePrefix), Is.True);
        }

        [Test]
        public async Task WhenCompletingMigrationToolStateIsArchived()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.Complete();

            var reader = new Raven4Adapter(ServerName, databaseName);
            var updatedToolState = await reader.GetDocument<RavenToolState>(RavenConstants.ToolStateId,
                (timeoutData, id) => { });

            var batches = await reader.GetDocuments<BatchInfo>((info => { return true;}), RavenConstants.BatchPrefix, (batch, id) => { });

            Assert.IsNull(updatedToolState);
            Assert.That(batches.Count, Is.EqualTo(0));
        }
    }
}