namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ITimeoutsSource
    {
        Task<IToolState> TryLoadOngoingMigration();
        Task<IToolState> Prepare(DateTime maxCutoffTime, string endpointName, IDictionary<string, string> runParameters);
        Task<IReadOnlyList<TimeoutData>> ReadBatch(int batchNumber);
        Task MarkBatchAsCompleted(int number);
        Task MarkBatchAsStaged(int number);
        Task Abort();
        Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTime cutOffTime);
        Task Complete();
        Task<bool> CheckIfAMigrationIsInProgress();
    }
}