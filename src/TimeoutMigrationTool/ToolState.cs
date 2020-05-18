namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Threading.Tasks;

    public class ToolState
    {
        public bool IsPrepared { get; private set; }

        public StorageInfo StorageInfo { get; private set; }

        public BatchInfo CurrentBatch { get; private set; }

        public bool HasMoreBatches {
            get
            {
                return CurrentBatch.Number == StorageInfo.NumberOfBatches;
            }
        }

        public Task MarkAsPrepared(StorageInfo storageInfo)
        {
            StorageInfo = storageInfo;

            CurrentBatch = new BatchInfo
            {
                Number = 0,
                State = BatchState.Pending
            };

            IsPrepared = true;

            //todo store

            return Task.CompletedTask;
        }

        public Task MarkCurrentBatchAsStaged()
        {
            CurrentBatch.State = BatchState.Staged;

            //todo store
            return Task.CompletedTask;
        }

        public Task CompleteCurrentBatch()
        {
            var newBatch = CurrentBatch.Number++;
            CurrentBatch = new BatchInfo
            {
                Number = newBatch,
                State = BatchState.Pending
            };

            //todo store
            return Task.CompletedTask;
        }
    }
}