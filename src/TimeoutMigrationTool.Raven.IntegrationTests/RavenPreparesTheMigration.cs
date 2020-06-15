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
            await testSuite.InitTimeouts(nrOfTimeouts);
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
            var timeoutStorage = new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var toolState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.That(toolState, Is.Null);
        }

        [Test]
        public async Task WhenLoadingAnOngoingMigrationAndWeFoundOneWeReturnIt()
        {
            await testSuite.SaveToolState(testSuite.SetupToolState(DateTime.Now.AddDays(-1)));

            var timeoutStorage = new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var retrievedToolState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.That(retrievedToolState, Is.Not.Null);
            Assert.That(retrievedToolState.NumberOfBatches, Is.GreaterThan(0));
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatches()
        {
            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), testSuite.EndpointName, new Dictionary<string, string>());

            Assert.That(toolState.NumberOfBatches, Is.EqualTo(2));

            var firstBatch = await toolState.TryGetNextBatch();

            var batchData = await timeoutStorage.ReadBatch(firstBatch.Number);

            Assert.That(batchData.Count(), Is.EqualTo(RavenConstants.DefaultPagingSize));

            firstBatch.State = BatchState.Completed;

            var nextBatch = await toolState.TryGetNextBatch();
            var nextBatchData = await timeoutStorage.ReadBatch(nextBatch.Number);

            Assert.That(nextBatchData.Count(), Is.EqualTo(nrOfTimeouts - RavenConstants.DefaultPagingSize));
        }

        [Test]
        public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatchesWhenMoreEndpointsAreAvailable()
        {
            testSuite.EndpointName = "B";
            await testSuite.InitTimeouts(nrOfTimeouts, true);

            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-1), testSuite.EndpointName, new Dictionary<string, string>());

            Assert.That(toolState.NumberOfBatches, Is.EqualTo(1));

            var nextBatch = await toolState.TryGetNextBatch();
            var nextBatchData = await timeoutStorage.ReadBatch(nextBatch.Number);
            Assert.That(nextBatchData.Count(), Is.EqualTo(500));
        }

        [Test]
        public async Task WhenStoringTheToolStateTheToolStateIsUpdated()
        {
            var toolState = testSuite.SetupToolState(DateTime.Now);
            await testSuite.SaveToolState(toolState);

            var timeoutStorage =
                new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);

            var updatedToolState = await timeoutStorage.TryLoadOngoingMigration();
            Assert.That(updatedToolState.EndpointName, Is.EqualTo(testSuite.EndpointName));
        }

        private readonly int nrOfTimeouts = 1500;
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