namespace Particular.TimeoutMigrationTool.SqlT
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class SqlTTimeoutCreator : ICreateTransportTimeouts
    {
        public Task<int> StageBatch(List<TimeoutData> timeouts)
        {
            throw new System.NotImplementedException();
        }

        public Task<int> CompleteBatch(int number)
        {
            throw new System.NotImplementedException();
        }

        public Task<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {

        }
    }
}