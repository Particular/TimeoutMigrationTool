using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.RavenDB;

namespace TimeoutMigrationTool.Raven4.Tests
{
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
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState).ConfigureAwait(false);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now.AddDays(-1));
            var batchToVerify = batches.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var timeoutDatasInBatch = await sut.ReadBatch(batchToVerify.Number);

            Assert.That(batchToVerify.TimeoutIds.Length, Is.EqualTo(timeoutDatasInBatch.Count));
        }

        [Test]
        public async Task WhenCompletingABatchCurrentBatchShouldBeMovedUp()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState).ConfigureAwait(false);

            var timeoutStorage =
                new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now.AddDays(-1));
            var batchToVerify = batches.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.CompleteBatch(batchToVerify.Number);

            var reader = new RavenDbReader(ServerName, databaseName, RavenDbVersion.Four);
            var updatedBatch = await reader.GetItem<BatchInfo>($"batch/{batchToVerify.Number}");
            toolState = await GetToolState();
            var currentBatch = toolState.GetCurrentBatch();
            Assert.That(updatedBatch.State, Is.EqualTo(BatchState.Completed));
            Assert.That(currentBatch.Number, Is.EqualTo(batchToVerify.Number + 1));
        }
    }
}