namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class RabbitMqTransportAdapter : ITransportAdapter
    {
        public RabbitMqTransportAdapter(string targetConnectionString)
        {
            this.targetConnectionString = targetConnectionString;
        }

        public Task CompleteBatch()
        {
            throw new System.NotImplementedException();
        }

        public Task StageBatch(List<TimeoutData> timeouts)
        {
            throw new System.NotImplementedException();
        }

        readonly string targetConnectionString;
    }
}