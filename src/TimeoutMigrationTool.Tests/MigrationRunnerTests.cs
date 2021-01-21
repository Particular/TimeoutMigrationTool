namespace TimeoutMigrationTool.Tests
{
    using System;
    using System.Collections.Generic;
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
            timeoutsSource = new FakeTimeoutsSource();
            timeoutsTarget = new FakeTimeoutTarget();
            logger = new ConsoleLogger(false);

            runner = new MigrationRunner(logger, timeoutsSource, timeoutsTarget);

            endpoints = new List<EndpointInfo>
            {
                new EndpointInfo
                {
                    EndpointName = "Sales",
                    NrOfTimeouts = 500
                }
            };
            testEndpoint = endpoints.First().EndpointName;
            timeoutsSource.SetupEndpoints(endpoints);
        }

        [Test]
        public async Task WhenRunningWithoutToolState()
        {
            var batches = GetBatches();
            timeoutsSource.SetupBatchesToPrepare(batches);
            timeoutsSource.SetupTimeoutsToReadForBatch(batches.First());

            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(timeoutsSource.EndpointsWereListed);
            Assert.That(timeoutsSource.ToolStateWasCreated);
            Assert.That(timeoutsTarget.EndpointWasVerified);
            Assert.That(timeoutsSource.BatchWasRead);
            Assert.That(timeoutsTarget.BatchWasStaged);
            Assert.That(timeoutsSource.BatchWasCompleted);
            Assert.That(timeoutsSource.ToolStateMovedToCompleted);
            Assert.That(timeoutsSource.MigrationWasAborted, Is.False);
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
            timeoutsSource.SetupToolStateToReturn(toolState);
            timeoutsSource.SetupEndpoints(new List<EndpointInfo>());

            Assert.ThrowsAsync<Exception>(async () =>
            {
                await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>
                {
                    { "someotherkey", "someothervalue" },
                    { "somekey", "someothervalue" }
                });
            });

            Assert.That(timeoutsSource.EndpointsWereListed, Is.False);
            Assert.That(timeoutsSource.ToolStateWasCreated, Is.False);
            Assert.That(timeoutsTarget.EndpointWasVerified, Is.False);
            Assert.That(timeoutsSource.BatchWasRead, Is.False);
            Assert.That(timeoutsTarget.BatchWasStaged, Is.False);
            Assert.That(timeoutsSource.BatchWasCompleted, Is.False);
            Assert.That(timeoutsSource.ToolStateMovedToCompleted, Is.False);
            Assert.That(timeoutsSource.MigrationWasAborted, Is.False);
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
            timeoutsSource.SetupToolStateToReturn(toolState);
            timeoutsSource.SetupTimeoutsToReadForBatch(batches.First());

            await runner.Run(DateTime.Now, EndpointFilter.SpecificEndpoint(testEndpoint), new Dictionary<string, string>());

            Assert.That(timeoutsSource.EndpointsWereListed, Is.False);
            Assert.That(timeoutsSource.ToolStateWasCreated, Is.False);
            Assert.That(timeoutsTarget.EndpointWasVerified, Is.False);
            Assert.That(timeoutsSource.BatchWasRead);
            Assert.That(timeoutsTarget.BatchWasStaged);
            Assert.That(timeoutsSource.BatchWasCompleted);
            Assert.That(timeoutsSource.ToolStateMovedToCompleted);
            Assert.That(timeoutsSource.MigrationWasAborted, Is.False);
        }

        static List<BatchInfo> GetBatches()
        {
            var batches = new List<BatchInfo>
            {
                new BatchInfo(1, BatchState.Pending, 1)
            };
            return batches;
        }

        FakeTimeoutsSource timeoutsSource;
        FakeTimeoutTarget timeoutsTarget;
        MigrationRunner runner;
        List<EndpointInfo> endpoints;
        string testEndpoint;
        Microsoft.Extensions.Logging.ILogger logger;
    }
}