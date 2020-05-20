using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool
{
    public interface ITimeoutStorage
    {
        Task<ToolState> GetToolState();
        Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime);
        Task<List<TimeoutData>> ReadBatch(int batchNumber);
        Task CompleteBatch(int number);
        Task StoreToolState(ToolState toolState);
    }
}