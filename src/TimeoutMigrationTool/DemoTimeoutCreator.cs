namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class DemoTimeoutCreator : ICreateTransportTimeouts
    {
        public Task CompleteBatch(int number)
        {
            return Task.CompletedTask;
        }

        public Task StageBatch(List<TimeoutData> timeouts)
        {
            return Task.CompletedTask;
        }
    }
}