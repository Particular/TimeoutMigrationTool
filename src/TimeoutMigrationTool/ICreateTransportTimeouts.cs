namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ICreateTransportTimeouts
    {
        Task<int> StageBatch(IReadOnlyList<TimeoutData> timeouts);
        Task<int> CompleteBatch(int number);
        Task<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint);
    }
}