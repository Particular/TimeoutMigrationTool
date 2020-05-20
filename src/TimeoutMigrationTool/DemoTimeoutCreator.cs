namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class DemoTimeoutCreator : ICreateTransportTimeouts
    {
        public Task CompleteBatch(int number)
        {
            return Task.Delay(3000);
        }

        public Task StageBatch(List<TimeoutData> timeouts)
        {
            return Task.Delay(3000);
        }
    }
}