using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Particular.TimeoutMigrationTool;

namespace TimeoutMigrationTool.Tests
{
    using System.Linq;

    public class FakeTimeoutStorage : ITimeoutStorage
    {
        private ToolState existingToolState;
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

        public Task<ToolState> TryLoadOngoingMigration()
        {
            return Task.FromResult(existingToolState);
        }

        public Task<ToolState> Prepare(DateTime maxCutoffTime, EndpointInfo endpoint, IDictionary<string, string> runParameters)
        {
            ToolStateWasCreated = true;
            var toolState = new ToolState(runParameters, endpoint, preparedBatches);
            return Task.FromResult(toolState);
        }

        public Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            BatchWasRead = true;
            var timeoutsInBatch = readBatchResults.First(x => x.Number == batchNumber).TimeoutIds.Length;

            var timeouts = new List<TimeoutData>(timeoutsInBatch);
            for (var i= 0; i < timeoutsInBatch; i++ )
                timeouts.Add(new TimeoutData());

            return Task.FromResult(timeouts);
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

        public Task<List<EndpointInfo>> ListEndpoints(DateTime cutOffTime)
        {
            EndpointsWereListed = true;
            return Task.FromResult(endpoints);
        }

        public void SetupToolStateToReturn(ToolState toolState)
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
    }
}