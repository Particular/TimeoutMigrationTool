namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutStorage
    {
        Task<ToolState> GetOrCreateToolState();
        Task<List<BatchInfo>> Prepare();
        Task<List<TimeoutData>> ReadBatch(int batchNumber);
        Task CompleteBatch(int number);
        Task StoreToolState(ToolState toolState);
    }
}