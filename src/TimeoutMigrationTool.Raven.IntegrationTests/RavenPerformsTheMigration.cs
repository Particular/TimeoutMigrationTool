namespace TimeoutMigrationTool.Raven.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;
    using Raven3;
    using Raven4;

    public abstract class RavenPerformsTheMigration
    {
        private readonly int nrOfTimeouts = 2000;
        IRavenTestSuite testSuite;

        [SetUp]
        public async Task Setup()
        {
            testSuite = CreateTestSuite();
            await testSuite.SetupDatabase();
            await testSuite.InitTimeouts(nrOfTimeouts);
        }

        [TearDown]
        public async Task TearDown()
        {
            await testSuite.TeardownDatabase();
        }

        protected abstract IRavenTestSuite CreateTestSuite();

        [Test]
        public async Task WhenReadingABatchAllTimeoutsInBatchAreReturned()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now.AddDays(-1), testSuite.EndpointName);

            var batchToVerify = batches.First();

            var sut = new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var timeoutDatasInBatch = await sut.ReadBatch(batchToVerify.Number);

            Assert.That(batchToVerify.TimeoutIds.Length, Is.EqualTo(timeoutDatasInBatch.Count));
        }

        [Test]
        public async Task WhenMarkingBatchAsStagedThenBatchStatusIsUpdated()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now.AddDays(-1), testSuite.EndpointName);

            var batchToVerify = batches.First();
            Assert.That(batchToVerify.State, Is.EqualTo(BatchState.Pending));

            var sut = new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.MarkBatchAsStaged(batchToVerify.Number);

            var updatedBatch = await testSuite.RavenAdapter.GetDocument<RavenBatch>($"{RavenConstants.BatchPrefix}/{batchToVerify.Number}", (batch, id) => { });

            Assert.That(updatedBatch.State, Is.EqualTo(BatchState.Staged));
        }

        [Test]
        public async Task WhenCompletingABatchCurrentBatchShouldBeMovedUp()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), testSuite.EndpointName, new Dictionary<string, string>());

            var batchToVerify = await toolState.TryGetNextBatch();

            var sut = new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.MarkBatchAsCompleted(batchToVerify.Number);

            var updatedBatch = await testSuite.RavenAdapter.GetDocument<RavenBatch>($"{RavenConstants.BatchPrefix}/{batchToVerify.Number}", (batch, id) => { });

            toolState = await sut.TryLoadOngoingMigration();
            var currentBatch = await toolState.TryGetNextBatch();

            Assert.That(updatedBatch.State, Is.EqualTo(BatchState.Completed));
            Assert.That(currentBatch, Is.Not.Null);
            Assert.That(currentBatch.Number, Is.EqualTo(batchToVerify.Number + 1));
        }

        [Test]
        public async Task WhenCompletingABatchTimeoutsAreMarkedDone()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), testSuite.EndpointName, new Dictionary<string, string>());

            var batchToVerify = await toolState.TryGetNextBatch();
            var timeoutIdToVerify = ((RavenToolState)toolState).Batches.First().TimeoutIds.First();

            var sut = new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.MarkBatchAsCompleted(batchToVerify.Number);

            var updatedTimeout = await testSuite.RavenAdapter.GetDocument<TimeoutData>(timeoutIdToVerify,
                (timeoutData, id) => { timeoutData.Id = id; });

            Assert.That(updatedTimeout.OwningTimeoutManager.StartsWith(RavenConstants.MigrationDonePrefix), Is.True);
        }

        [Test]
        public async Task WhenCompletingMigrationToolStateIsArchived()
        {
            var toolState = testSuite.SetupToolState(DateTime.Now);
            await testSuite.SaveToolState(toolState);

            var timeStarted = DateTime.UtcNow;

            var sut = new RavenDBTimeoutStorage(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.Complete();

            var updatedToolState = await testSuite.RavenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId,
                (timeoutData, id) => { });

            Assert.IsNull(updatedToolState);

            var batches = await testSuite.RavenAdapter.GetDocuments<RavenBatch>((info => { return true; }), RavenConstants.BatchPrefix, (batch, id) => { });
            Assert.That(batches.Count, Is.EqualTo(0));

            var archivedToolStates = await testSuite.RavenAdapter.GetDocuments<RavenToolStateDto>(_ => true, RavenConstants.ArchivedToolStateIdPrefix, (batch, id) => { });

            Assert.That(archivedToolStates.Count, Is.EqualTo(1));

            var archivedToolState = archivedToolStates.Single();
            Assert.That(archivedToolState.Status, Is.EqualTo(MigrationStatus.Completed));
            Assert.That(archivedToolState.CompletedAt, Is.GreaterThan(timeStarted));
        }
    }

    [TestFixture]
    public class Raven4PerformsTheMigration : RavenPerformsTheMigration
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven4TestSuite();
        }
    }

    [TestFixture]
    public class Raven3PerformsTheMigration : RavenPerformsTheMigration
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven3TestSuite();
        }
    }
}