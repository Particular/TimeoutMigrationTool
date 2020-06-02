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
            _timeoutStorage = new FakeTimeoutStorage();
            transportTimeoutsCreator = new FakeTransportTimeoutCreator();
            runner = new MigrationRunner(_timeoutStorage, transportTimeoutsCreator);

            endpoints = new List<EndpointInfo>
            {
                new EndpointInfo
                {
                    EndpointName = "Sales",
                    NrOfTimeouts = 500
                }
            };
            testEndpoint = endpoints.First();
            _timeoutStorage.SetupEndpoints(endpoints);
        }

        [Test]
        public async Task WhenRunningWithoutToolState()
        {
            _timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>
            {
                new BatchInfo
                {
                    Number = 1,
                    State = BatchState.Pending,
                    TimeoutIds = new[] {"timeouts/1"}
                }
            });
            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(_timeoutStorage.BatchesWerePrepared);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(_timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(_timeoutStorage.BatchWasCompleted);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateNeverRun()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), testEndpoint)
            {
                Status = MigrationStatus.NeverRun
            };
            _timeoutStorage.SetupToolStateToReturn(toolState);

            _timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>
            {
                new BatchInfo
                {
                    Number = 1,
                    State = BatchState.Pending,
                    TimeoutIds = new[] {"timeouts/1"}
                }
            });
            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(_timeoutStorage.BatchesWerePrepared);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(_timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(_timeoutStorage.BatchWasCompleted);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateStoragePreparedAndParametersMismatch()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), testEndpoint)
            {
                Status = MigrationStatus.StoragePrepared,
                Endpoint = new EndpointInfo
                {
                    EndpointName = "Invoicing"
                }
            };
            _timeoutStorage.SetupToolStateToReturn(toolState);

            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled, Is.False);
            Assert.That(_timeoutStorage.BatchesWerePrepared, Is.False);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared, Is.False);
            Assert.That(_timeoutStorage.BatchWasRead, Is.False);
            Assert.That(transportTimeoutsCreator.BatchWasStaged, Is.False);
            Assert.That(_timeoutStorage.BatchWasCompleted, Is.False);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted, Is.False);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
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
            _timeoutStorage.SetupToolStateToReturn(toolState);

            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled, Is.False);
            Assert.That(_timeoutStorage.BatchesWerePrepared, Is.False);
            Assert.That(_timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(_timeoutStorage.BatchWasCompleted);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateCompleted()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), testEndpoint)
            {
                Status = MigrationStatus.Completed,
                Endpoint = testEndpoint
            };
            _timeoutStorage.SetupToolStateToReturn(toolState);
            _timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>
            {
                new BatchInfo
                {
                    Number = 1,
                    State = BatchState.Pending,
                    TimeoutIds = new[] {"timeouts/1"}
                }
            });

            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(_timeoutStorage.BatchesWerePrepared);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(_timeoutStorage.BatchWasRead);
            Assert.That(transportTimeoutsCreator.BatchWasStaged);
            Assert.That(_timeoutStorage.BatchWasCompleted);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenPrepareDoesNotReturnAnyBatches()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), testEndpoint)
            {
                Status = MigrationStatus.Completed,
                Endpoint = testEndpoint
            };
            _timeoutStorage.SetupToolStateToReturn(toolState);
            _timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>());

            await runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.BatchesWerePrepared);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(_timeoutStorage.BatchWasRead, Is.False);
            Assert.That(transportTimeoutsCreator.BatchWasStaged, Is.False);
            Assert.That(_timeoutStorage.BatchWasCompleted, Is.False);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        FakeTimeoutStorage _timeoutStorage;
        FakeTransportTimeoutCreator transportTimeoutsCreator;
        MigrationRunner runner;
        List<EndpointInfo> endpoints;
        EndpointInfo testEndpoint;
    }
}