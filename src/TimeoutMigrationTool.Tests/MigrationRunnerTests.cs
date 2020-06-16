namespace TimeoutMigrationTool.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;

    [TestFixture]
    public class MigrationRunnerTests
    {
        [SetUp]
        public void Setup()
        {
            timeoutStorage = new FakeTimeoutStorage();
            transportTimeoutsCreator = new FakeTransportTimeoutCreator();
            logger = new ConsoleLogger(false);

            runner = new MigrationRunner(logger, timeoutStorage, transportTimeoutsCreator);

            endpoints = new List<EndpointInfo>
            {
                new EndpointInfo
                {
                    EndpointName = "Sales",
                    NrOfTimeouts = 500
                }
            };
            testEndpoint = endpoints.First().EndpointName;
            timeoutStorage.SetupEndpoints(endpoints);
        }

        [Test]
        public async Task WhenRunningWithoutToolState()
        {
            var batches = GetBatches();
            timeoutStorage.SetupBatchesToPrepare(batches);
            timeoutStorage.SetupTimeoutsToReadForBatch(batches.First());

            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(timeoutStorage.EndpointsWereListed);
            Assert.That(timeoutStorage.ToolStateWasCreated);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(timeoutStorage.BatchWasCompleted);
            Assert.That(timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public void WhenRunningWithStateStoragePreparedAndParametersMismatch()
        {
            var toolState = new FakeToolState
            {
                Batches = GetBatches(),
                EndpointName = "Invoicing",
                RunParameters = new Dictionary<string, string> { { "somekey", "somevalue" } }
            };
            timeoutStorage.SetupToolStateToReturn(toolState);
            timeoutStorage.SetupEndpoints(new List<EndpointInfo>());

            Assert.ThrowsAsync<Exception>(async () =>
            {
                await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());
            });

            Assert.That(timeoutStorage.EndpointsWereListed, Is.False);
            Assert.That(timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified, Is.False);
            Assert.That(timeoutStorage.BatchWasRead, Is.False);
            Assert.That(transportTimeoutsCreator.BatchWasStaged, Is.False);
            Assert.That(timeoutStorage.BatchWasCompleted, Is.False);
            Assert.That(timeoutStorage.ToolStateMovedToCompleted, Is.False);
            Assert.That(timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateStoragePreparedAndParametersMatch()
        {
            var batches = GetBatches();
            var toolState = new FakeToolState
            {
                EndpointName = testEndpoint,
                Batches = batches,
                RunParameters = new Dictionary<string, string>()
            };
            timeoutStorage.SetupToolStateToReturn(toolState);
            timeoutStorage.SetupTimeoutsToReadForBatch(batches.First());

            await runner.Run(DateTime.Now, EndpointFilter.SpecificEndpoint(testEndpoint), new Dictionary<string, string>());

            Assert.That(timeoutStorage.EndpointsWereListed, Is.False);
            Assert.That(timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified, Is.False);
            Assert.That(timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(timeoutStorage.BatchWasCompleted);
            Assert.That(timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(timeoutStorage.ToolStateWasAborted, Is.False);
        }

        static List<BatchInfo> GetBatches()
        {
            var batches = new List<BatchInfo>
            {
                new BatchInfo(1, BatchState.Pending, 1)
            };
            return batches;
        }

        FakeTimeoutStorage timeoutStorage;
        FakeTransportTimeoutCreator transportTimeoutsCreator;
        MigrationRunner runner;
        List<EndpointInfo> endpoints;
        string testEndpoint;
        Microsoft.Extensions.Logging.ILogger logger;
    }
}