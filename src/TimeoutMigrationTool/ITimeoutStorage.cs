﻿namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutStorage
    {
        Task<ToolState> GetOrCreateToolState();
        Task<StorageInfo> Prepare();
        Task<List<TimeoutData>> ReadBatch(int batchNumber);
    }
}