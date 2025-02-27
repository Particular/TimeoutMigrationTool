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
            var timeoutStorage = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var toolState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.That(toolState, Is.Null);
        }

        [Test]
        public async Task WhenLoadingAnOngoingMigrationAndWeFoundOneWeReturnIt()
        {
            await testSuite.InitTimeouts(nrOfTimeouts);
            await testSuite.SaveToolState(testSuite.SetupToolState(DateTime.Now.AddDays(-1)));

            var timeoutStorage = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            var retrievedToolState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.That(retrievedToolState, Is.Not.Null);
            Assert.That(retrievedToolState.NumberOfBatches, Is.GreaterThan(0));
        }

        [Test]
        public async Task WhenPrepareDiesHalfWayThroughWhenRunWithDifferentParametersThrowsException()
        {
            nrOfTimeouts = 10;
            testSuite.EndpointName = "EndpointA";
            await testSuite.InitTimeouts(nrOfTimeouts, "EndpointA", 0);

            var cutoffTime = DateTime.Now.AddDays(-1);
            var timeoutStorage =
                new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);
            await timeoutStorage.Prepare(cutoffTime, testSuite.EndpointName, new Dictionary<string, string>());

            var toolState = await testSuite.RavenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId);
            toolState.Status = MigrationStatus.Preparing;
            await testSuite.RavenAdapter.UpdateDocument(RavenConstants.ToolStateId, toolState);

            var secondBatchNrOfTimeouts = 5;
            await testSuite.InitTimeouts(secondBatchNrOfTimeouts, "EndpointA", nrOfTimeouts);

            Assert.ThrowsAsync<Exception>(async () => { await timeoutStorage.Prepare(cutoffTime, "someOtherEndpoint", new Dictionary<string, string>()); });
            var timeoutsFromSecondBatch = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(data => Convert.ToInt32(data.Id.Replace("TimeoutDatas/", "")) >= nrOfTimeouts, "TimeoutDatas", (doc, id) => doc.Id = id);
            Assert.Multiple(() =>
            {
                Assert.That(timeoutsFromSecondBatch.Count(), Is.EqualTo(secondBatchNrOfTimeouts));
                Assert.That(timeoutsFromSecondBatch.All(x => !x.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix)), Is.True);
            });
        }

        [Test]
        public async Task WhenStoringTheToolStateTheToolStateIsUpdated()
        {
            await testSuite.InitTimeouts(nrOfTimeouts);
            var toolState = testSuite.SetupToolState(DateTime.Now);
            await testSuite.SaveToolState(toolState);

            var timeoutStorage =
                new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, false);

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
                new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var startTime = DateTime.UtcNow;
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), testSuite.EndpointName, new Dictionary<string, string>());

            Assert.That(toolState.NumberOfBatches, Is.EqualTo(2));
            var storedToolState = await testSuite.RavenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId);

            Assert.Multiple(() =>
            {
                Assert.That(storedToolState.NumberOfTimeouts, Is.EqualTo(nrOfTimeouts));
                Assert.That(storedToolState.NumberOfBatches, Is.EqualTo(2));
                Assert.That(storedToolState.Status, Is.EqualTo(MigrationStatus.StoragePrepared));
                Assert.That(storedToolState.StartedAt, Is.GreaterThan(startTime));
            });

            var firstBatch = await toolState.TryGetNextBatch();
            var batchData = await timeoutStorage.ReadBatch(firstBatch.Number);
            Assert.That(batchData.Count(), Is.EqualTo(RavenConstants.DefaultPagingSize));

            firstBatch.State = BatchState.Completed;
            await timeoutStorage.MarkBatchAsCompleted(firstBatch.Number);

            var nextBatch = await toolState.TryGetNextBatch();
            var nextBatchData = await timeoutStorage.ReadBatch(nextBatch.Number);

            Assert.That(nextBatchData.Count(), Is.EqualTo(nrOfTimeouts - RavenConstants.DefaultPagingSize));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatchesWhenMoreEndpointsAreAvailable(bool useIndex)
        {
            await testSuite.InitTimeouts(2000, testSuite.EndpointName, 0);
            await testSuite.InitTimeouts(1000, "SomeOtherEndpoint", 2000);
            await testSuite.CreateLegacyTimeoutManagerIndex(useIndex);

            var timeoutStorage =
                new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), testSuite.EndpointName, new Dictionary<string, string>());

            var batch = await toolState.TryGetNextBatch();
            var timeoutsInBatches = new List<string>();
            while (batch != null)
            {
                var batchData = await timeoutStorage.ReadBatch(batch.Number);
                timeoutsInBatches.AddRange(batchData.Select(x => x.Id));
                batch.State = BatchState.Completed;
                await timeoutStorage.MarkBatchAsCompleted(batch.Number);
                batch = await toolState.TryGetNextBatch();
            }

            Assert.That(timeoutsInBatches.Distinct().Count(), Is.EqualTo(2000));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenPrepareDiesHalfWayThroughWeCanStillSuccessfullyPrepare(bool useIndex)
        {
            nrOfTimeouts = 10;
            await testSuite.InitTimeouts(nrOfTimeouts, testSuite.EndpointName, 0);
            await testSuite.CreateLegacyTimeoutManagerIndex(useIndex);

            var cutoffTime = DateTime.Now.AddDays(-1);
            var timeoutStorage =
                new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            await timeoutStorage.Prepare(cutoffTime, testSuite.EndpointName, new Dictionary<string, string>());

            var toolStateDto = await testSuite.RavenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId);
            toolStateDto.Status = MigrationStatus.Preparing;
            await testSuite.RavenAdapter.UpdateDocument(RavenConstants.ToolStateId, toolStateDto);

            var secondBatchNrOfTimeouts = 5;
            await testSuite.InitTimeouts(secondBatchNrOfTimeouts, testSuite.EndpointName, nrOfTimeouts);
            await testSuite.EnsureIndexIsNotStale();
            var toolState = await timeoutStorage.Prepare(cutoffTime, testSuite.EndpointName, new Dictionary<string, string>());

            Assert.That(toolState.NumberOfBatches, Is.EqualTo(2));
            var firstBatch = await timeoutStorage.ReadBatch(1);
            var secondBatch = await timeoutStorage.ReadBatch(2);
            Assert.Multiple(() =>
            {
                Assert.That(firstBatch.Count(), Is.EqualTo(10));
                Assert.That(secondBatch.Count(), Is.EqualTo(secondBatchNrOfTimeouts));
                Assert.That(secondBatch.All(t => Convert.ToInt32(t.Id.Replace("TimeoutDatas/", "")) >= 10));
                Assert.That(firstBatch.All(t => Convert.ToInt32(t.Id.Replace("TimeoutDatas/", "")) < 10));
            });
        }

        int nrOfTimeouts = 1500;
    }

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.Raven3Url)]
    public class Raven3PreparesTheMigration : RavenPreparesTheMigration
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven3TestSuite();
        }
    }

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.Raven4Url)]
    public class Raven4PreparesTheMigration : RavenPreparesTheMigration
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven4TestSuite();
        }
    }
}