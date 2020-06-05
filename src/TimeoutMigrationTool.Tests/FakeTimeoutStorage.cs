using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Particular.TimeoutMigrationTool;

namespace TimeoutMigrationTool.Tests
{
    using System.Linq;

    public class FakeTimeoutStorage : ITimeoutStorage
    {
        private ToolState toolState;
        public int TimesToolStateWasStored { get; private set; } = 0;
        private List<BatchInfo> preparedBatches = new List<BatchInfo>();
        private List<EndpointInfo> endpoints = new List<EndpointInfo>();
        private List<BatchInfo> readBatchResults = new List<BatchInfo>();
        private bool canPrepareStorage;
        public bool BatchesWerePrepared { get; private set; }
        public bool BatchWasRead { get; private set; }
        public bool BatchWasCompleted { get; private set; }
        public bool BatchWasStaged { get; private set; }
        public bool ToolStateWasAborted { get; private set; }
        public bool ToolStateWasStored { get; private set; }
        public bool EndpointsWereListed { get; private set; }
        public bool CanPrepareStorageWasCalled { get; private set; }
        public bool ToolStateWasCreated { get; private set; }
        public bool ToolStateMovedToStoragePrepared { get; private set; }
        public bool ToolStateMovedToCompleted { get; private set; }

        public Task<ToolState> GetToolState()
        {
            return Task.FromResult(toolState);
        }

        public Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime, EndpointInfo endpoint)
        {
            BatchesWerePrepared = true;
            return Task.FromResult(preparedBatches);
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

        public Task StoreToolState(ToolState toolState)
        {
            ToolStateWasStored = true;
            TimesToolStateWasStored++;
            if (toolState.Status == MigrationStatus.NeverRun) ToolStateWasCreated = true;
            if (toolState.Status == MigrationStatus.StoragePrepared) ToolStateMovedToStoragePrepared = true;
            if (toolState.Status == MigrationStatus.Completed) ToolStateMovedToCompleted = true;
            return Task.CompletedTask;
        }

        public Task Abort(ToolState toolState)
        {
            ToolStateWasAborted = true;
            return Task.CompletedTask;
        }

        public Task<bool> CanPrepareStorage()
        {
            CanPrepareStorageWasCalled = true;
            return Task.FromResult(canPrepareStorage);
        }

        public Task<List<EndpointInfo>> ListEndpoints(DateTime cutOffTime)
        {
            EndpointsWereListed = true;
            return Task.FromResult(endpoints);
        }

        public void SetupToolStateToReturn(ToolState toolState)
        {
            this.toolState = toolState;
        }

        public void SetupBatchesToPrepare(List<BatchInfo> batches)
        {
            this.preparedBatches = batches;
        }

        public void SetupEndpoints(List<EndpointInfo> endpoints)
        {
            this.endpoints = endpoints;
        }

        public void SetupCanPrepareStorageResult(bool canPrepareStorage)
        {
            this.canPrepareStorage = canPrepareStorage;
        }

        public void SetupTimeoutsToReadForBatch(BatchInfo batchInfo)
        {
            this.readBatchResults.Add(batchInfo);
        }
    }
}