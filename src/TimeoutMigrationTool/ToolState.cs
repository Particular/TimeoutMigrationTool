namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Threading.Tasks;

    public class ToolState
    {
        public bool IsStoragePrepared { get; private set; }

        public StorageInfo StorageInfo { get; private set; }

        public BatchInfo CurrentBatch { get; private set; }

        public bool HasMoreBatches {
            get
            {
                return CurrentBatch.Number == StorageInfo.NumberOfBatches;
            }
        }

        public Task MarkStorageAsPrepared(StorageInfo storageInfo)
        {
            StorageInfo = storageInfo;

            CurrentBatch = new BatchInfo
            {
                Number = 0,
                State = BatchState.Pending
            };

            IsStoragePrepared = true;

            //TODO: store, need to figure out how to get the storage interface in here

            return Task.CompletedTask;
        }

        public Task MarkCurrentBatchAsStaged()
        {
            CurrentBatch.State = BatchState.Staged;

            //TODO: store, need to figure out how to get the storage interface in here
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

            //TODO: store, need to figure out how to get the storage interface in here
            return Task.CompletedTask;
        }
    }
}