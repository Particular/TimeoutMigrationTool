using System;
using System.ComponentModel;
using RabbitMQ.Client;

namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class RabbitMqTimeoutCreator : ICreateTransportTimeouts
    {
        public RabbitMqTimeoutCreator(string targetConnectionString)
        {
            this.targetConnectionString = targetConnectionString;
            this.batchWriter = new RabbitBatchWriter(targetConnectionString);
            this.messagePump = new RabbitStagePump(targetConnectionString, QueueCreator.StagingQueueName);
            Init();
        }

        private void Init()
        {
            this.factory = new ConnectionFactory();
            factory.Uri = new Uri(this.targetConnectionString);

            CreateStagingQueue();
        }

        private void CreateStagingQueue()
        {
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                QueueCreator.CreateStagingInfrastructure(channel);
            }
        }

        public Task StageBatch(List<TimeoutData> timeouts)
        {
            return this.batchWriter.WriteTimeoutsToStagingQueue(timeouts, QueueCreator.StagingQueueName);
        }

        public Task CompleteBatch(int number)
        {
            return messagePump.CompleteBatch(number);
        }

        private readonly RabbitStagePump messagePump;
        private readonly RabbitBatchWriter batchWriter;
        readonly string targetConnectionString;
        private ConnectionFactory factory;
    }
}