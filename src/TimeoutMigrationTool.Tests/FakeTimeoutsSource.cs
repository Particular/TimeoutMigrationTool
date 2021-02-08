namespace TimeoutMigrationTool.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Particular.TimeoutMigrationTool;

    public class FakeTimeoutsSource : ITimeoutsSource
    {
        IToolState existingToolState;
        List<BatchInfo> preparedBatches = new List<BatchInfo>();
        List<EndpointInfo> endpoints = new List<EndpointInfo>();
        List<BatchInfo> readBatchResults = new List<BatchInfo>();
        public bool BatchWasRead { get; private set; }
        public bool BatchWasCompleted { get; private set; }
        public bool BatchWasStaged { get; private set; }
        public bool MigrationWasAborted { get; private set; }
        public bool EndpointsWereListed { get; private set; }
        public bool ToolStateWasCreated { get; private set; }
        public bool ToolStateMovedToCompleted { get; private set; }

        public Func<Task<bool>> CheckIfMigrationIsInProgressFunc { get; set; }

        public FakeTimeoutsSource()
        {
            CheckIfMigrationIsInProgressFunc = () => Task.FromResult(existingToolState != null);
        }

        public Task<IToolState> TryLoadOngoingMigration()
        {
            return Task.FromResult(existingToolState);
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
            {
                timeouts.Add(new TimeoutData());
            }

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
            MigrationWasAborted = true;
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTime cutOffTime)
        {
            EndpointsWereListed = true;
            return Task.FromResult<IReadOnlyList<EndpointInfo>>(endpoints);
        }

        public void SetupToolStateToReturn(IToolState toolState)
        {
            existingToolState = toolState;
        }

        public void SetupBatchesToPrepare(List<BatchInfo> batches)
        {
            preparedBatches = batches;
        }

        public void SetupEndpoints(List<EndpointInfo> endpoints)
        {
            this.endpoints = endpoints;
        }

        public void SetupTimeoutsToReadForBatch(BatchInfo batchInfo)
        {
            readBatchResults.Add(batchInfo);
        }

        public Task Complete()
        {
            ToolStateMovedToCompleted = true;
            return Task.CompletedTask;
        }

        public Task<bool> CheckIfAMigrationIsInProgress() => CheckIfMigrationIsInProgressFunc();
    }
}