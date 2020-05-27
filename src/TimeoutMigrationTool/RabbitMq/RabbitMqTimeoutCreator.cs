namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System;
    using RabbitMQ.Client;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class RabbitMqTimeoutCreator : ICreateTransportTimeouts
    {
        public RabbitMqTimeoutCreator(string targetConnectionString)
        {
            this.targetConnectionString = targetConnectionString;
            batchWriter = new RabbitBatchWriter(targetConnectionString);
            messagePump = new RabbitStagePump(targetConnectionString, QueueCreator.StagingQueueName);
        }

        public async Task<bool> AbleToMigrate(string endpointName)
        {
            factory = new ConnectionFactory();
            factory.Uri = new Uri(targetConnectionString);

            await CreateStagingQueue();

            return await VerifyEndpointIsReadyForNativeTimeouts(endpointName);
        }

        public Task StageBatch(List<TimeoutData> timeouts)
        {
            return batchWriter.WriteTimeoutsToStagingQueue(timeouts, QueueCreator.StagingQueueName);
        }

        public Task CompleteBatch(int number)
        {
            return messagePump.CompleteBatch(number);
        }

        Task<bool> VerifyEndpointIsReadyForNativeTimeouts(string endpointName)
        {
            throw new NotImplementedException();
        }

        Task CreateStagingQueue()
        {
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                QueueCreator.CreateStagingInfrastructure(channel);
            }

            return Task.CompletedTask;
        }

        string targetConnectionString;
        ConnectionFactory factory;

        readonly RabbitStagePump messagePump;
        readonly RabbitBatchWriter batchWriter;
    }
}