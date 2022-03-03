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

    public abstract class RavenAbortsTheMigration
    {
        readonly int nrOfTimeouts = 1500;
        IRavenTestSuite testSuite;

        [SetUp]
        public async Task Setup()
        {
            testSuite = CreateTestSuite();
            await testSuite.SetupDatabase();
        }

        [TearDown]
        public async Task TearDown()
        {
            await testSuite.TeardownDatabase();
        }

        protected abstract IRavenTestSuite CreateTestSuite();

        [Test]
        public async Task WhenThereIsStateAbortShouldDeleteState()
        {
            var toolState = testSuite.SetupToolState(DateTime.Now);
            await testSuite.SaveToolState(toolState);

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.Abort();

            var storedSate = await testSuite.GetToolState();
            Assert.That(storedSate, Is.Null);
        }

        [Test]
        public async Task WhenAbortingOnPreparedStorageStateShouldBeCleanedUp()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            var storage = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await storage.Prepare(cutOffTime, testSuite.EndpointName, new Dictionary<string, string>());

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.Abort();

            var storedSate = await testSuite.GetToolState();
            Assert.That(storedSate, Is.Null);
        }

        [Test]
        public async Task WhenAbortingWithAToolStateInPreparingToolWillStillCleanupBatches()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            await testSuite.InitTimeouts(nrOfTimeouts);

            var storage = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await storage.Prepare(cutOffTime, testSuite.EndpointName, new Dictionary<string, string>());

            var toolState = await testSuite.RavenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId);
            toolState.Status = MigrationStatus.Preparing;
            await testSuite.RavenAdapter.UpdateDocument(RavenConstants.ToolStateId, toolState);

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.Abort();

            var batchesInStore = await testSuite.RavenAdapter.GetDocuments<RavenBatch>(batch => true, RavenConstants.BatchPrefix);
            var hiddenTimeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(timeout => timeout.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix), RavenConstants.DefaultTimeoutPrefix, (timeout, id) => { timeout.Id = id; });
            Assert.That(batchesInStore.Count, Is.EqualTo(0));
            Assert.That(hiddenTimeouts.Count, Is.EqualTo(0));
        }

        [Test]
        public async Task WhenAbortingWithAToolStateInStoragePreparedToolWillStillCleanupBatches()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            await testSuite.InitTimeouts(nrOfTimeouts);

            var storage = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await storage.Prepare(cutOffTime, testSuite.EndpointName, new Dictionary<string, string>());

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.Abort();

            var batchesInStore = await testSuite.RavenAdapter.GetDocuments<RavenBatch>(batch => true, RavenConstants.BatchPrefix);
            var hiddenTimeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(timeout => timeout.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix), RavenConstants.DefaultTimeoutPrefix, (timeout, id) => { timeout.Id = id; });
            Assert.That(batchesInStore.Count, Is.EqualTo(0));
            Assert.That(hiddenTimeouts.Count, Is.EqualTo(0));
        }

        [Test]
        public async Task WhenCheckingIfThereIsSomethingToAbortAndThereIsAToolState()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            await testSuite.InitTimeouts(nrOfTimeouts);

            var storage = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await storage.Prepare(cutOffTime, testSuite.EndpointName, new Dictionary<string, string>());

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var migrationIsInProgress = await sut.CheckIfAMigrationIsInProgress();

            Assert.That(migrationIsInProgress, Is.True);
        }

        [Test]
        public async Task WhenCheckingIfThereIsSomethingToAbortInACleanSystem()
        {
            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var migrationIsInProgress = await sut.CheckIfAMigrationIsInProgress();

            Assert.That(migrationIsInProgress, Is.False);
        }

        [Test]
        public async Task WhenCleaningUpBatchesThenTimeoutsInIncompleteBatchesAreReset()
        {
            await testSuite.InitTimeouts(nrOfTimeouts);
            testSuite.SetupToolState(DateTime.Now.AddDays(-1));
            var preparedBatches = await testSuite.SetupExistingBatchInfoInDatabase();
            var incompleteBatches = preparedBatches.Skip(1).Take(1).ToList();
            var incompleteBatch = incompleteBatches.First();

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.CleanupExistingBatchesAndResetTimeouts(preparedBatches, incompleteBatches);

            var incompleteBatchFromStorage = await testSuite.RavenAdapter.GetDocument<RavenBatch>($"{RavenConstants.BatchPrefix}/{incompleteBatch.Number}");
            var resetTimeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(x => incompleteBatch.TimeoutIds.Contains(x.Id), "TimeoutDatas", (doc, id) => doc.Id = id);

            Assert.That(incompleteBatchFromStorage, Is.Null);
            Assert.That(resetTimeouts.Select(t => t.OwningTimeoutManager), Is.All.Matches<string>(x => !x.StartsWith(RavenConstants.MigrationOngoingPrefix)));
        }

        [Test]
        public async Task WhenCleaningUpBatchesThenTimeoutsInCompleteBatchesAreNotReset()
        {
            await testSuite.InitTimeouts(nrOfTimeouts);
            testSuite.SetupToolState(DateTime.Now.AddDays(-1));
            var preparedBatches = await testSuite.SetupExistingBatchInfoInDatabase();
            var incompleteBatches = preparedBatches.Skip(1).Take(1).ToList();
            var completeBatch = preparedBatches.First();

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await sut.CleanupExistingBatchesAndResetTimeouts(preparedBatches, incompleteBatches);

            var completeBatchFromStorage = await testSuite.RavenAdapter.GetDocument<RavenBatch>($"{RavenConstants.BatchPrefix}/{completeBatch.Number}", (doc, id) => { });
            var resetTimeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(x => completeBatch.TimeoutIds.Contains(x.Id), "TimeoutDatas", (doc, id) => doc.Id = id);

            Assert.That(completeBatchFromStorage, Is.Null);
            Assert.That(resetTimeouts.Select(t => t.OwningTimeoutManager), Is.All.Matches<string>(x => x.StartsWith(RavenConstants.MigrationOngoingPrefix)));
        }
    }

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.Raven4Url)]
    public class Raven4AbortsTheMigration : RavenAbortsTheMigration
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven4TestSuite();
        }
    }

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.Raven3Url)]
    public class Raven3AbortsTheMigration : RavenAbortsTheMigration
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven3TestSuite();
        }
    }
}