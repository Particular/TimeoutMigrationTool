﻿namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutStorage
    {
        Task<ToolState> GetToolState();
        Task<StorageInfo> Prepare();
        Task<List<TimeoutData>> ReadBatch(int batchNumber);
        Task CompleteBatch(int number);
        Task StoreToolState(ToolState toolState);
    }
}