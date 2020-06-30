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
            timeoutStorage = new FakeTimeoutStorage();
            logger = new ConsoleLogger(false);

            runner = new AbortRunner(logger, timeoutStorage);

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
            timeoutStorage.SetupToolStateToReturn(toolState);

            await runner.Run();

            Assert.That(timeoutStorage.ToolStateWasAborted, Is.True);
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
        AbortRunner runner;
        List<EndpointInfo> endpoints;
        string testEndpoint;
        Microsoft.Extensions.Logging.ILogger logger;
    }
}