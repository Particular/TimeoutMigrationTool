namespace TimeoutMigrationTool.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;

    [TestFixture]
    public class AbortRunnerTests
    {
        [SetUp]
        public void Setup()
        {
            timeoutsSource = new FakeTimeoutsSource();
            logger = new ConsoleLogger(false);

            runner = new AbortRunner(logger, timeoutsSource);

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
        public void WhenAbortingAndTimeoutStorageFoundNothingToAbortThrowsException()
        {
            Assert.ThrowsAsync<Exception>(async () => { await runner.Run(); });
        }

        [Test]
        public async Task WhenAbortingAndTimeoutStorageFoundToolStateItIsAborted()
        {
            var batches = GetBatches();
            var toolState = new FakeToolState
            {
                EndpointName = testEndpoint,
                Batches = batches,
                RunParameters = new Dictionary<string, string>()
            };
            timeoutsSource.SetupToolStateToReturn(toolState);

            await runner.Run();

            Assert.That(timeoutsSource.ToolStateWasAborted, Is.True);
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
        AbortRunner runner;
        List<EndpointInfo> endpoints;
        string testEndpoint;
        Microsoft.Extensions.Logging.ILogger logger;
    }
}