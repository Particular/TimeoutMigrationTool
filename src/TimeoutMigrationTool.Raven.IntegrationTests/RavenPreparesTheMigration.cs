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

    public abstract class RavenPreparesTheMigration
    {
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
        public async Task WhenGettingTimeoutStateAndNoneIsFoundNullIsReturned()
        {
            await testSuite.InitTimeouts(nrOfTimeouts);
            var timeoutStorage = new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var toolState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.That(toolState, Is.Null);
        }

        [Test]
        public async Task WhenLoadingAnOngoingMigrationAndWeFoundOneWeReturnIt()
        {
            await testSuite.InitTimeouts(nrOfTimeouts);
            await testSuite.SaveToolState(testSuite.SetupToolState(DateTime.Now.AddDays(-1)));

            var timeoutStorage = new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var retrievedToolState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.That(retrievedToolState, Is.Not.Null);
            Assert.That(retrievedToolState.NumberOfBatches, Is.GreaterThan(0));
        }

        [Test]
        public async Task WhenPrepareDiesHalfWayThroughWhenRunWithDifferentParametersThrowsException()
        {
            nrOfTimeouts = 10;
            await testSuite.InitTimeouts(nrOfTimeouts);

            var cutoffTime = DateTime.Now.AddDays(-1);
            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await timeoutStorage.Prepare(cutoffTime, testSuite.EndpointName, new Dictionary<string, string>());
            await testSuite.RavenAdapter.DeleteDocument(RavenConstants.ToolStateId);

            var secondBatchNrOfTimeouts = 5;
            await testSuite.InitTimeouts(secondBatchNrOfTimeouts, false, "secondBatch");

            Assert.ThrowsAsync<Exception>(async () => { await timeoutStorage.Prepare(cutoffTime, "someOtherEndpoint", new Dictionary<string, string>()); });
            var timeoutsFromSecondBatch = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(data => data.Id.Contains("secondBatch"), "TimeoutDatas", (doc, id) => doc.Id = id);
            Assert.That(timeoutsFromSecondBatch.Count(), Is.EqualTo(secondBatchNrOfTimeouts));
            Assert.That(timeoutsFromSecondBatch.All(x => !x.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix)), Is.True);
        }

        [Test]
        public async Task WhenStoringTheToolStateTheToolStateIsUpdated()
        {
            await testSuite.InitTimeouts(nrOfTimeouts);
            var toolState = testSuite.SetupToolState(DateTime.Now);
            await testSuite.SaveToolState(toolState);

            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);

            var updatedToolState = await timeoutStorage.TryLoadOngoingMigration();
            Assert.That(updatedToolState.EndpointName, Is.EqualTo(testSuite.EndpointName));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatches(bool useIndex)
        {
            nrOfTimeouts = RavenConstants.DefaultPagingSize + 5;
            await testSuite.InitTimeouts(nrOfTimeouts);
            await testSuite.CreateLegacyTimeoutManagerIndex(useIndex);
            
            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var startTime = DateTime.UtcNow;
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), testSuite.EndpointName, new Dictionary<string, string>());

            Assert.That(toolState.NumberOfBatches, Is.EqualTo(2));
            var storedToolState = await testSuite.RavenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId, (batch, id) => { });

            Assert.That(storedToolState.NumberOfTimeouts, Is.EqualTo(nrOfTimeouts));
            Assert.That(storedToolState.NumberOfBatches, Is.EqualTo(2));
            Assert.That(storedToolState.Status, Is.EqualTo(MigrationStatus.StoragePrepared));
            Assert.That(storedToolState.StartedAt, Is.GreaterThan(startTime));

            var firstBatch = await toolState.TryGetNextBatch();
            var batchData = await timeoutStorage.ReadBatch(firstBatch.Number);
            Assert.That(batchData.Count(), Is.EqualTo(RavenConstants.DefaultPagingSize));

            await timeoutStorage.MarkBatchAsCompleted(firstBatch.Number);

            var nextBatch = await toolState.TryGetNextBatch();
            var nextBatchData = await timeoutStorage.ReadBatch(nextBatch.Number);

            Assert.That(nextBatchData.Count(), Is.EqualTo(nrOfTimeouts - RavenConstants.DefaultPagingSize));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatchesWhenMoreEndpointsAreAvailable(bool useIndex)
        {
            nrOfTimeouts = 3000;
            testSuite.EndpointName = "B";
            await testSuite.InitTimeouts(nrOfTimeouts, true);
            await testSuite.CreateLegacyTimeoutManagerIndex(useIndex);

            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), testSuite.EndpointName, new Dictionary<string, string>());

            var batch = await toolState.TryGetNextBatch();
            var timeoutsInBatches = new List<string>();
            while (batch != null)
            {
                var batchData = await timeoutStorage.ReadBatch(batch.Number);
                timeoutsInBatches.AddRange(batchData.Select(x => x.Id));
                await timeoutStorage.MarkBatchAsCompleted(batch.Number);
                batch = await toolState.TryGetNextBatch();
            }

            Assert.That(timeoutsInBatches.Distinct().Count(), Is.EqualTo(3000/2));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenPrepareDiesHalfWayThroughWeCanStillSuccessfullyPrepare(bool useIndex)
        {
            nrOfTimeouts = 10;
            await testSuite.InitTimeouts(nrOfTimeouts);
            await testSuite.CreateLegacyTimeoutManagerIndex(useIndex);

            var cutoffTime = DateTime.Now.AddDays(-1);
            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            await timeoutStorage.Prepare(cutoffTime, testSuite.EndpointName, new Dictionary<string, string>());
            await testSuite.RavenAdapter.DeleteDocument(RavenConstants.ToolStateId);

            var secondBatchNrOfTimeouts = 5;
            await testSuite.InitTimeouts(secondBatchNrOfTimeouts, false, "secondBatch");
            await testSuite.EnsureIndexIsNotStale();
            var toolState = await timeoutStorage.Prepare(cutoffTime, testSuite.EndpointName, new Dictionary<string, string>());

            Assert.That(toolState.NumberOfBatches, Is.EqualTo(2));
            var firstBatch = await timeoutStorage.ReadBatch(1);
            var secondBatch = await timeoutStorage.ReadBatch(2);
            Assert.That(firstBatch.Count(), Is.EqualTo(10));
            Assert.That(secondBatch.Count(), Is.EqualTo(secondBatchNrOfTimeouts));
            Assert.That(secondBatch.All(t => t.Id.Contains("secondBatch")));
            Assert.That(firstBatch.All(t => !t.Id.Contains("secondBatch")));
        }

        int nrOfTimeouts = 1500;
    }

    [TestFixture]
    public class Raven3PreparesTheMigration : RavenPreparesTheMigration
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven3TestSuite();
        }
    }

    [TestFixture]
    public class Raven4PreparesTheMigration : RavenPreparesTheMigration
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven4TestSuite();
        }
    }
}