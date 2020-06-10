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
            testEndpoint = endpoints.First();
            timeoutStorage.SetupEndpoints(endpoints);
        }

        [Test]
        public async Task WhenRunningWithoutToolState()
        {
            timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>
            {
                new BatchInfo
                {
                    Number = 1,
                    State = BatchState.Pending,
                    TimeoutIds = new[] {"timeouts/1"}
                }
            });
            timeoutStorage.SetupCanPrepareStorageResult(true);
            var batchInfo = new BatchInfo
            {
                Number = 1,
                State = BatchState.Pending,
                TimeoutIds = new[] {"timeouts/1"}
            };
            timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>
            {
                batchInfo
            });
            timeoutStorage.SetupTimeoutsToReadForBatch(batchInfo);

            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(timeoutStorage.EndpointsWereListed);
            Assert.That(timeoutStorage.ToolStateWasCreated);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(timeoutStorage.BatchWasCompleted);
            Assert.That(timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateNeverRun()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), testEndpoint)
            {
                Status = MigrationStatus.NeverRun
            };
            timeoutStorage.SetupToolStateToReturn(toolState);
            timeoutStorage.SetupCanPrepareStorageResult(true);
            var batchInfo = new BatchInfo
            {
                Number = 1,
                State = BatchState.Pending,
                TimeoutIds = new[] {"timeouts/1"}
            };
            timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>
            {
                batchInfo
            });
            timeoutStorage.SetupTimeoutsToReadForBatch(batchInfo);
            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(timeoutStorage.EndpointsWereListed);
            Assert.That(timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(timeoutStorage.BatchWasCompleted);
            Assert.That(timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public void WhenRunningWithStateStoragePreparedAndParametersMismatch()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), testEndpoint)
            {
                Status = MigrationStatus.StoragePrepared,
                Endpoint = new EndpointInfo
                {
                    EndpointName = "Invoicing"
                }
            };
            timeoutStorage.SetupToolStateToReturn(toolState);

            Assert.ThrowsAsync<Exception>(async () =>
            {
                await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());
            });

            Assert.That(timeoutStorage.EndpointsWereListed);
            Assert.That(timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(timeoutStorage.CanPrepareStorageWasCalled, Is.False);
            Assert.That(timeoutStorage.ToolStateMovedToStoragePrepared, Is.False);
            Assert.That(timeoutStorage.BatchWasRead, Is.False);
            Assert.That(transportTimeoutsCreator.BatchWasStaged, Is.False);
            Assert.That(timeoutStorage.BatchWasCompleted, Is.False);
            Assert.That(timeoutStorage.ToolStateMovedToCompleted, Is.False);
            Assert.That(timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateStoragePreparedAndParametersMatch()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), testEndpoint)
            {
                Status = MigrationStatus.StoragePrepared,
                Endpoint = testEndpoint
            };
            toolState.InitBatches(new List<BatchInfo>
            {
                new BatchInfo
                {
                    Number = 1,
                    State = BatchState.Pending,
                    TimeoutIds = new[] {"timeouts/1"}
                }
            });
            timeoutStorage.SetupToolStateToReturn(toolState);
            timeoutStorage.SetupCanPrepareStorageResult(true);
            var batchInfo = new BatchInfo
            {
                Number = 1,
                State = BatchState.Pending,
                TimeoutIds = new[] {"timeouts/1"}
            };
            timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>
            {
                batchInfo
            });
            timeoutStorage.SetupTimeoutsToReadForBatch(batchInfo);

            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(timeoutStorage.EndpointsWereListed);
            Assert.That(timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(timeoutStorage.CanPrepareStorageWasCalled, Is.False);
            Assert.That(timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(timeoutStorage.BatchWasCompleted);
            Assert.That(timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public void WhenRunningWithStateCompleted()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), testEndpoint)
            {
                Status = MigrationStatus.Completed,
                Endpoint = testEndpoint
            };

            timeoutStorage.SetupToolStateToReturn(toolState);

            Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>()));
        }

        FakeTimeoutStorage timeoutStorage;
        FakeTransportTimeoutCreator transportTimeoutsCreator;
        MigrationRunner runner;
        List<EndpointInfo> endpoints;
        EndpointInfo testEndpoint;
        Microsoft.Extensions.Logging.ILogger logger;
    }
}