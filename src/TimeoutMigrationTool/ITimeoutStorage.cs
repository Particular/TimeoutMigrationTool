namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutStorage
    {
        Task<ToolState> GetToolState();
        Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime, EndpointInfo endpoint);
        Task<List<TimeoutData>> ReadBatch(int batchNumber);
        Task MarkBatchAsCompleted(int number);
        Task MarkBatchAsStaged(int number);
        Task StoreToolState(ToolState toolState);
        Task Abort();
        Task<bool> CanPrepareStorage();
        Task<List<EndpointInfo>> ListEndpoints(DateTime cutOffTime);
        Task Complete();
    }
}