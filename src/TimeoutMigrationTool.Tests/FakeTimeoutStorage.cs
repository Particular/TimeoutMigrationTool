namespace TimeoutMigrationTool.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Particular.TimeoutMigrationTool;

    public class FakeTimeoutStorage : ITimeoutStorage
    {
        private IToolState existingToolState;
        private List<BatchInfo> preparedBatches = new List<BatchInfo>();
        private List<EndpointInfo> endpoints = new List<EndpointInfo>();
        private List<BatchInfo> readBatchResults = new List<BatchInfo>();
        public bool BatchWasRead { get; private set; }
        public bool BatchWasCompleted { get; private set; }
        public bool BatchWasStaged { get; private set; }
        public bool ToolStateWasAborted { get; private set; }
        public bool EndpointsWereListed { get; private set; }
        public bool ToolStateWasCreated { get; private set; }
        public bool ToolStateMovedToCompleted { get; private set; }

        public Task<IToolState> TryLoadOngoingMigration()
        {
            return Task.FromResult<IToolState>(existingToolState);
        }

        public Task<IToolState> Prepare(DateTime maxCutoffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            ToolStateWasCreated = true;

            return Task.FromResult<IToolState>(new FakeToolState
            {
                RunParameters = runParameters,
                EndpointName = endpointName,
                Batches = preparedBatches
            });
        }

        public Task<IReadOnlyList<TimeoutData>> ReadBatch(int batchNumber)
        {
            BatchWasRead = true;
            var timeoutsInBatch = readBatchResults.First(x => x.Number == batchNumber).NumberOfTimeouts;

            var timeouts = new List<TimeoutData>(timeoutsInBatch);
            for (var i = 0; i < timeoutsInBatch; i++)
                timeouts.Add(new TimeoutData());

            return Task.FromResult<IReadOnlyList<TimeoutData>>(timeouts);
        }

        public Task MarkBatchAsCompleted(int number)
        {
            BatchWasCompleted = true;
            return Task.CompletedTask;
        }

        public Task MarkBatchAsStaged(int number)
        {
            BatchWasStaged = true;
            return Task.CompletedTask;
        }

        public Task Abort()
        {
            ToolStateWasAborted = true;
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTime cutOffTime)
        {
            EndpointsWereListed = true;
            return Task.FromResult<IReadOnlyList<EndpointInfo>>(endpoints);
        }

        public void SetupToolStateToReturn(IToolState toolState)
        {
            this.existingToolState = toolState;
        }

        public void SetupBatchesToPrepare(List<BatchInfo> batches)
        {
            this.preparedBatches = batches;
        }

        public void SetupEndpoints(List<EndpointInfo> endpoints)
        {
            this.endpoints = endpoints;
        }

        public void SetupTimeoutsToReadForBatch(BatchInfo batchInfo)
        {
            this.readBatchResults.Add(batchInfo);
        }

        public Task Complete()
        {
            ToolStateMovedToCompleted = true;
            return Task.CompletedTask;
        }

        public Task<bool> CheckIfAMigrationIsInProgress()
        {
            return Task.FromResult(this.existingToolState != null);
        }
    }
}