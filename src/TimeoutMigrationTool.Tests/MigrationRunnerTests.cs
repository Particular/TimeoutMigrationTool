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
            _transportTimeoutsCreator = new FakeTransportTimeoutCreator();
            _runner = new MigrationRunner(_timeoutStorage, _transportTimeoutsCreator);

            _endpoints = new List<EndpointInfo>
            {
                new EndpointInfo
                {
                    EndpointName = "Sales",
                    NrOfTimeouts = 500
                }
            };
            _testEndpoint = _endpoints.First();
            _timeoutStorage.SetupEndpoints(_endpoints);
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
            await _runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated);
            Assert.That(_transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(_timeoutStorage.BatchesWerePrepared);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(_timeoutStorage.BatchWasRead);
            Assert.That(_transportTimeoutsCreator.BatchWasStaged);
            Assert.That(_timeoutStorage.BatchWasCompleted);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateNeverRun()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), _testEndpoint)
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
            await _runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(_transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(_timeoutStorage.BatchesWerePrepared);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(_timeoutStorage.BatchWasRead);
            Assert.That(_transportTimeoutsCreator.BatchWasStaged);
            Assert.That(_timeoutStorage.BatchWasCompleted);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateStoragePreparedAndParametersMismatch()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), _testEndpoint)
            {
                Status = MigrationStatus.StoragePrepared,
                Endpoint = new EndpointInfo
                {
                    EndpointName = "Invoicing"
                }
            };
            _timeoutStorage.SetupToolStateToReturn(toolState);

            await _runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(_transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled, Is.False);
            Assert.That(_timeoutStorage.BatchesWerePrepared, Is.False);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared, Is.False);
            Assert.That(_timeoutStorage.BatchWasRead, Is.False);
            Assert.That(_transportTimeoutsCreator.BatchWasStaged, Is.False);
            Assert.That(_timeoutStorage.BatchWasCompleted, Is.False);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted, Is.False);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateStoragePreparedAndParametersMatch()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), _testEndpoint)
            {
                Status = MigrationStatus.StoragePrepared,
                Endpoint = _testEndpoint
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

            await _runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated, Is.False);
            Assert.That(_transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled, Is.False);
            Assert.That(_timeoutStorage.BatchesWerePrepared, Is.False);
            Assert.That(_timeoutStorage.BatchWasRead);
            Assert.That(_transportTimeoutsCreator.BatchWasStaged);
            Assert.That(_timeoutStorage.BatchWasCompleted);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenRunningWithStateCompleted()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), _testEndpoint)
            {
                Status = MigrationStatus.Completed,
                Endpoint = _testEndpoint
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

            await _runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated);
            Assert.That(_transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(_timeoutStorage.BatchesWerePrepared);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(_timeoutStorage.BatchWasRead);
            Assert.That(_transportTimeoutsCreator.BatchWasStaged);
            Assert.That(_timeoutStorage.BatchWasCompleted);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        [Test]
        public async Task WhenPrepareDoesNotReturnAnyBatches()
        {
            var toolState = new ToolState(new Dictionary<string, string>(), _testEndpoint)
            {
                Status = MigrationStatus.Completed,
                Endpoint = _testEndpoint
            };
            _timeoutStorage.SetupToolStateToReturn(toolState);
            _timeoutStorage.SetupBatchesToPrepare(new List<BatchInfo>());

            await _runner.Run(DateTime.Now, EndpointFilter.IncludeAll, new Dictionary<string, string>());

            Assert.That(_timeoutStorage.EndpointsWereListed);
            Assert.That(_timeoutStorage.ToolStateWasCreated);
            Assert.That(_timeoutStorage.CanPrepareStorageWasCalled);
            Assert.That(_transportTimeoutsCreator.EndpointWasVerified);
            Assert.That(_timeoutStorage.BatchesWerePrepared);
            Assert.That(_timeoutStorage.ToolStateMovedToStoragePrepared);
            Assert.That(_timeoutStorage.BatchWasRead, Is.False);
            Assert.That(_transportTimeoutsCreator.BatchWasStaged, Is.False);
            Assert.That(_timeoutStorage.BatchWasCompleted, Is.False);
            Assert.That(_timeoutStorage.ToolStateMovedToCompleted);
            Assert.That(_timeoutStorage.ToolStateWasAborted, Is.False);
        }

        private FakeTimeoutStorage _timeoutStorage;
        private FakeTransportTimeoutCreator _transportTimeoutsCreator;
        private MigrationRunner _runner;
        private List<EndpointInfo> _endpoints;
        private EndpointInfo _testEndpoint;
    }
}