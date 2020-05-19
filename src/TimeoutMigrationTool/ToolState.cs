namespace Particular.TimeoutMigrationTool
{
    public class ToolState
    {
        public bool IsStoragePrepared { get; set; }

        public StorageInfo StorageInfo { get; set; }

        public BatchInfo CurrentBatch { get; set; }

        public bool HasMoreBatches
        {
            get
            {
                return CurrentBatch.Number < StorageInfo.NumberOfBatches;
            }
        }
    }
}