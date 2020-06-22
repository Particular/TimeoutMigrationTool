namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface ICreateTransportTimeouts
    {
        Task<int> StageBatch(List<TimeoutData> timeouts);
        Task<int> CompleteBatch(int number);
        Task<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint);
    }
}