namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class RabbitMqTimeoutCreator : ICreateTransportTimeouts
    {
        public RabbitMqTimeoutCreator(string targetConnectionString)
        {
            this.targetConnectionString = targetConnectionString;
        }

        public Task StageBatch(List<TimeoutData> timeouts)
        {
            throw new System.NotImplementedException();
        }

        public Task CompleteBatch(int number)
        {
            throw new System.NotImplementedException();
        }

        readonly string targetConnectionString;
    }
}