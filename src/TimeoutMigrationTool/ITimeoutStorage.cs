namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutStorage
    {
        Task<ToolState> TryLoadOngoingMigration();
        Task<ToolState> Prepare(DateTime maxCutoffTime, string endpointName, IDictionary<string, string> runParameters);
        Task<List<TimeoutData>> ReadBatch(int batchNumber);
        Task MarkBatchAsCompleted(int number);
        Task MarkBatchAsStaged(int number);
        Task Abort();
        Task<List<EndpointInfo>> ListEndpoints(DateTime cutOffTime);
        Task Complete();
    }
}